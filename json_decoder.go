package hekalocal

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

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
	TimestampField  string `toml:"timestamp_field"`
	UUIDField       string `toml:"uuid_field"`
	TypeField       string `toml:"type_field"`
	LoggerField     string `toml:"logger_field"`
	EnvVersionField string `toml:"env_version_field"`
	HostnameField   string `toml:"hostname_field"`
	SeverityField   string `toml:"severity_field"`
	PIDField        string `toml:"pid_field"`

	// The message payload will be hashed and made into a UUID along with the timestamp.
	HashUUID bool `toml:"hash_uuid"`

	fieldMap map[string]fieldDecoder
}

// Init is provided to make JSONDecoder implement the Heka pipeline.Plugin interface.
func (jd *JSONDecoder) Init(config interface{}) (err error) {
	jd.config = config.(*JSONDecoderConfig)
	jd.config.buildFieldMap()
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

func (jd *JSONDecoder) decodeJSON(jsonStr string, msg *message.Message) (err error) {
	rawMap := make(map[string]json.RawMessage)
	if err = json.Unmarshal([]byte(jsonStr), &rawMap); err != nil {
		return
	}

	for key, raw := range rawMap {
		var field *message.Field
		rawS := string(raw)
		rb := rune(rawS[0])

		// If it's a number, string, or bool, decode it.
		if unicode.IsDigit(rb) || rb == '-' || rb == '"' || rawS == "true" || rawS == "false" {
			var val interface{}
			if err = json.Unmarshal(raw, &val); err != nil {
				return
			}
			field, err = message.NewField(key, val, "")
		} else {
			// If it's an object, array, or null, leave it as encoded JSON.
			field, err = message.NewField(key, []byte(raw), "json")
		}
		if err != nil {
			return
		}

		if fieldFn, ok := jd.config.fieldMap[key]; ok {
			err = fieldFn(msg, field)
			if err != nil {
				return
			}
			continue
		}
		msg.AddField(field)
	}
	return
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
		return err
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
		return fmt.Errorf("Not a valid UUID: %s", field.String())
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
