package hekalocal

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

// JSONDecoder parses JSON message payloads and fills their contents into the message fields. It
// also optionally fills in the Timestamp, UUID, and Type message fields.
type JSONDecoder struct {
	config *JSONDecoderConfig
}

type fieldDecoder func(*message.Message, *message.Field) error

// JSONDecoderConfig contains the optional field names from which to extract message fields.
type JSONDecoderConfig struct {
	TimestampField   string            `toml:"timestamp_field"`
	UUIDField        string            `toml:"uuid_field"`
	TypeField        string            `toml:"type_field"`
	LoggerField      string            `toml:"logger_field"`
	EnvVersionField  string            `toml:"env_version_field"`
	HostnameField    string            `toml:"hostname_field"`
	SeverityField    string            `toml:"severity_field"`
	PIDField         string            `toml:"pid_field"`
	Flatten          bool              `toml:"flatten"`
	FlattenPrefix    string            `toml:"flatten_prefix"`
	FlattenToStrings bool              `toml:"flatten_to_strings"`
	MoveFields       map[string]string `toml:"move_fields"`
	KeepFields       []string          `toml:"keep_fields"`
	RemoveFields     []string          `toml:"remove_fields"`

	// The message payload will be hashed and made into a UUID along with the timestamp.
	HashUUID bool `toml:"hash_uuid"`

	fieldMap map[string]fieldDecoder
}

// Init is provided to make JSONDecoder implement the Heka pipeline.Plugin interface.
func (jd *JSONDecoder) Init(config interface{}) (err error) {
	jd.config = config.(*JSONDecoderConfig)
	jd.config.buildFieldMap()
	if jd.config.MoveFields == nil {
		jd.config.MoveFields = make(map[string]string)
	}
	for _, path := range jd.config.KeepFields {
		jd.config.MoveFields[path] = path
	}
	for _, path := range jd.config.RemoveFields {
		jd.config.MoveFields[path] = ""
	}
	return
}

// ConfigStruct is provided to make JSONDecoder implement the Heka pipeline.HasConfigStruct interface.
func (jd *JSONDecoder) ConfigStruct() interface{} {
	return new(JSONDecoderConfig)
}

// Decode is provided to make JSONDecoder implement the Heka pipeline.Decoder interface.
func (jd *JSONDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack, err error) {
	packs = []*pipeline.PipelinePack{pack}
	payload := pack.Message.GetPayload()
	err = jd.decodeJSON(payload, pack.Message)
	if jd.config.HashUUID {
		hash := md5.Sum([]byte(payload))
		pack.Message.SetUuid([]byte(NewTimestampUUID(pack.Message.GetTimestamp(), hash[0:])))
	}
	return
}

func addDecodeError(msg *message.Message, jsonErr error) (err error) {
	var field *message.Field
	if field, err = message.NewField("decode_error", jsonErr.Error(), ""); err != nil {
		return err
	}
	msg.AddField(field)
	if field, err = message.NewField("payload", msg.GetPayload(), ""); err != nil {
		return err
	}
	msg.AddField(field)
	return nil
}

func (jd *JSONDecoder) decodeJSON(jsonStr string, msg *message.Message) error {
	var err error
	rawMap := make(map[string]interface{})
	if err := json.Unmarshal([]byte(jsonStr), &rawMap); err != nil {
		return addDecodeError(msg, err)
	}

	moveMap := make(map[string]interface{}, len(jd.config.MoveFields))
	for from, to := range jd.config.MoveFields {
		val, exists := dottedRemove(rawMap, from)
		if !exists {
			continue
		}
		if to != "" {
			moveMap[to] = val
		}
	}

	if jd.config.Flatten {
		rawMap = jd.flattenJSON(rawMap)
		if jd.config.FlattenPrefix != "" && len(rawMap) > 0 {
			rawMap = map[string]interface{}{jd.config.FlattenPrefix: rawMap}
		}
	}

	for to, val := range moveMap {
		err = dottedSet(rawMap, to, val)
		if err != nil {
			addDecodeError(msg, err)
		}
	}

	for key, val := range rawMap {
		var field *message.Field
		switch val.(type) {
		case nil:
			// message.NewField crashes if you give it a nil value.
			field, err = message.NewField(key, []byte("null"), "json")
		case map[string]interface{}, []interface{}:
			enc, _ := json.Marshal(val)
			field, err = message.NewField(key, enc, "json")
		default:
			field, err = message.NewField(key, val, "")
		}

		if err != nil {
			return err
		}

		if fieldFn, ok := jd.config.fieldMap[key]; ok {
			err = fieldFn(msg, field)
			if err != nil {
				return err
			}
			continue
		}
		msg.AddField(field)
	}
	return nil
}

func dottedSet(m map[string]interface{}, path string, val interface{}) error {
	keys := strings.Split(path, ".")
	var key string
	var ok bool
	for len(keys) > 1 {
		key = keys[0]
		keys = keys[1:]
		subM, exists := m[key]
		if !exists {
			subM = map[string]interface{}{}
			m[key] = subM
		}
		if m, ok = subM.(map[string]interface{}); !ok {
			return fmt.Errorf("Key does not refer to an object: %s", key)
		}
	}
	m[keys[0]] = val
	return nil
}

func dottedRemove(m map[string]interface{}, path string) (interface{}, bool) {
	keys := strings.SplitN(path, ".", 2)
	val, exists := m[keys[0]]
	if !exists {
		return nil, false
	}

	// At the last key, remove and return the value.
	if len(keys) == 1 {
		delete(m, keys[0])
		return val, true
	}

	subTable, ok := val.(map[string]interface{})
	if !ok {
		return nil, false
	}

	val, exists = dottedRemove(subTable, keys[1])
	if !exists {
		return nil, false
	}
	if len(subTable) == 0 {
		delete(m, keys[0])
	}
	return val, true
}

func (jd *JSONDecoder) flattenJSON(j map[string]interface{}) map[string]interface{} {
	flat := make(map[string]interface{}, len(j))
	jd.doFlattenJSON(j, flat, "")
	return flat
}

func (jd *JSONDecoder) doFlattenJSON(j, flat map[string]interface{}, prefix string) {
	for key, val := range j {
		pkey := prefix + key
		switch t := val.(type) {
		case []interface{}:
			if jd.config.FlattenToStrings {
				val = iSliceToStrings(t)
			}
		case map[string]interface{}:
			jd.doFlattenJSON(t, flat, pkey+".")
			continue
		default:
			if jd.config.FlattenToStrings {
				val = iToString(val)
			}
		}
		flat[pkey] = val
	}
}

func iSliceToStrings(s []interface{}) []interface{} {
	ss := make([]interface{}, 0, len(s))
	for _, val := range s {
		ss = append(ss, iToString(val))
	}
	return ss
}

func iToString(val interface{}) string {
	if str, ok := val.(string); ok {
		return str
	}
	enc, _ := json.Marshal(val)
	return string(enc)
}

func (conf *JSONDecoderConfig) buildFieldMap() {
	conf.fieldMap = make(map[string]fieldDecoder)
	for _, f := range []struct {
		name string
		fn   func(*message.Message, *message.Field) error
	}{
		{conf.TimestampField, conf.decodeTimestamp},
		{conf.UUIDField, conf.decodeUUID},
		{conf.SeverityField, conf.decodeSeverity},
		{conf.TypeField, conf.decodeStringField((*message.Message).SetType)},
		{conf.LoggerField, conf.decodeStringField((*message.Message).SetLogger)},
		{conf.EnvVersionField, conf.decodeStringField((*message.Message).SetEnvVersion)},
		{conf.HostnameField, conf.decodeStringField((*message.Message).SetHostname)},
		{conf.PIDField, conf.decodeIntField((*message.Message).SetPid)},
	} {
		if f.name != "" {
			conf.fieldMap[f.name] = f.fn
		}
	}
}

func (conf *JSONDecoderConfig) decodeTimestamp(msg *message.Message, field *message.Field) error {
	var (
		timestamp time.Time
		err       error
	)
	switch *(field.ValueType) {
	case message.Field_STRING:
		timestamp, err = message.ForgivingTimeParse(time.RFC3339, field.GetValueString()[0], time.UTC)
	case message.Field_DOUBLE:
		v := field.GetValueDouble()[0]

		// Anything with < 14 digits is *probably* epoch seconds rather than microseconds.
		if v < 10000000000000 {
			v *= 1000000000
		}

		// time.Unix takes seconds and microseconds, so convert microseconds to those.
		timestamp = time.Unix(int64(v)/1000000000, int64(v)%1000000000)
	default:
		return nil
	}

	if err != nil {
		return addDecodeError(msg, fmt.Errorf("Invalid timestamp: %s", err.Error()))
	}
	msg.SetTimestamp(timestamp.UnixNano())
	return nil
}

func (conf *JSONDecoderConfig) decodeUUID(msg *message.Message, field *message.Field) error {
	var u uuid.UUID

	if *(field.ValueType) == message.Field_STRING {
		u = uuid.Parse(field.GetValueString()[0])
	}

	if u == nil {
		return addDecodeError(msg, fmt.Errorf("Not a valid UUID: %s", field.String()))
	}
	msg.SetUuid(u)
	return nil
}

var severityMap = []struct {
	name     string
	severity int32
}{
	{"alert", 1},
	{"crit", 2},
	{"err", 3},
	{"warn", 4},
	{"notice", 5},
	{"info", 6},
	{"debug", 7},
	{"emerg", 0},
}

func (conf *JSONDecoderConfig) decodeSeverity(msg *message.Message, field *message.Field) error {
	switch *(field.ValueType) {
	case message.Field_DOUBLE:
		msg.SetSeverity(int32(field.GetValueDouble()[0]))
	case message.Field_STRING:
		level := strings.ToLower(field.GetValueString()[0])
		for _, s := range severityMap {
			if strings.HasPrefix(level, s.name) || strings.HasPrefix(s.name, level) {
				msg.SetSeverity(s.severity)
				break
			}
		}
	}
	return nil
}

func (conf *JSONDecoderConfig) decodeStringField(setter func(*message.Message, string)) fieldDecoder {
	return func(msg *message.Message, field *message.Field) error {
		if *field.ValueType == message.Field_STRING {
			v := field.GetValueString()[0]
			if v != "" {
				setter(msg, v)
			}
		}
		return nil
	}
}

func (conf *JSONDecoderConfig) decodeIntField(setter func(*message.Message, int32)) fieldDecoder {
	return func(msg *message.Message, field *message.Field) error {
		if *field.ValueType == message.Field_DOUBLE {
			v := field.GetValueDouble()[0]
			if v != 0 {
				setter(msg, int32(v))
			}
		}
		return nil
	}
}

func init() {
	pipeline.RegisterPlugin("JSONDecoder", func() interface{} { return new(JSONDecoder) })
}
