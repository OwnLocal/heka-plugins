package hekalocal

import (
	"bytes"
	"encoding/json"
	"time"

	"code.google.com/p/go-uuid/uuid"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/plugins/elasticsearch"
)

// JSONEncoder serializes messages to JSON.
type JSONEncoder struct {
	config *JSONEncoderConfig
	coord  *elasticsearch.ElasticSearchCoordinates
}

type fieldEncoder func(map[string]interface{}, *message.Message)

// JSONEncoderConfig contains the optional field names to put Message fields into.
type JSONEncoderConfig struct {
	TimestampField  string `toml:"timestamp_field"`
	UUIDField       string `toml:"uuid_field"`
	SeverityField   string `toml:"severity_field"`
	TypeField       string `toml:"type_field"`
	LoggerField     string `toml:"logger_field"`
	EnvVersionField string `toml:"env_version_field"`
	HostnameField   string `toml:"hostname_field"`
	PIDField        string `toml:"pid_field"`

	ElasticsearchBulk  bool   `toml:"elasticsearch_bulk"`
	ElasticsearchIndex string `toml:"elasticsearch_index"`
	ElasticsearchType  string `toml:"elasticsearch_type"`
	ElasticsearchID    string `toml:"elasticsearch_id"`

	fieldMap map[string]fieldEncoder
}

// ConfigStruct is provided to make JSONEncoder implement the Heka pipeline.HasConfigStruct interface.
func (enc *JSONEncoder) ConfigStruct() interface{} {
	return &JSONEncoderConfig{
		ElasticsearchIndex: "heka-%{2006.01.02}",
		ElasticsearchType:  "%{Type}",
		ElasticsearchID:    "%{UUID}",
	}
}

// Init is provided to make JSONEncoder implement the Heka pipeline.Plugin interface.
func (enc *JSONEncoder) Init(config interface{}) (err error) {
	enc.config = config.(*JSONEncoderConfig)
	enc.config.buildFieldMap()
	enc.coord = &elasticsearch.ElasticSearchCoordinates{
		Index:                enc.config.ElasticsearchIndex,
		Type:                 enc.config.ElasticsearchType,
		Id:                   enc.config.ElasticsearchID,
		ESIndexFromTimestamp: true,
	}
	return
}

// Encode is implemented to make JSONEncoder implement the pipeline.Encoder interface.
func (enc *JSONEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	rawMap := make(map[string]interface{})
	for _, field := range pack.Message.GetFields() {
		if field.GetValueType() == message.Field_BYTES && field.GetRepresentation() == "json" {
			rawMap[field.GetName()] = (*json.RawMessage)(&field.GetValueBytes()[0])
		} else {
			rawMap[field.GetName()] = field.GetValue()
		}
	}

	for _, encodeFn := range enc.config.fieldMap {
		encodeFn(rawMap, pack.Message)
	}

	buf := &bytes.Buffer{}
	if enc.config.ElasticsearchBulk {
		enc.coord.PopulateBuffer(pack.Message, buf)
		buf.WriteString("\n")
	}

	jsonEnc := json.NewEncoder(buf)
	err = jsonEnc.Encode(rawMap)
	output = buf.Bytes()
	return
}

func (conf *JSONEncoderConfig) encodeTimestamp(rawMap map[string]interface{}, msg *message.Message) {
	if msg.Timestamp != nil {
		rawMap[conf.TimestampField] = time.Unix(0, *msg.Timestamp).UTC()
	}
}

func (conf *JSONEncoderConfig) encodeUUID(rawMap map[string]interface{}, msg *message.Message) {
	if msg.Uuid != nil {
		rawMap[conf.UUIDField] = uuid.UUID(msg.Uuid).String()
	}
}

func (conf *JSONEncoderConfig) encodeSeverity(rawMap map[string]interface{}, msg *message.Message) {
	rawMap[conf.SeverityField] = msg.GetSeverity()
}

func (conf *JSONEncoderConfig) encodePID(rawMap map[string]interface{}, msg *message.Message) {
	if msg.Pid != nil {
		rawMap[conf.PIDField] = msg.GetPid()
	}
}

func (conf *JSONEncoderConfig) buildFieldMap() {
	conf.fieldMap = make(map[string]fieldEncoder)
	for _, f := range []struct {
		name string
		fn   fieldEncoder
	}{
		{conf.TimestampField, conf.encodeTimestamp},
		{conf.UUIDField, conf.encodeUUID},
		{conf.SeverityField, conf.encodeSeverity},
		{conf.PIDField, conf.encodePID},
		{conf.TypeField, conf.encodeStringField(conf.TypeField, (*message.Message).GetType)},
		{conf.LoggerField, conf.encodeStringField(conf.LoggerField, (*message.Message).GetLogger)},
		{conf.EnvVersionField, conf.encodeStringField(conf.EnvVersionField, (*message.Message).GetEnvVersion)},
		{conf.HostnameField, conf.encodeStringField(conf.HostnameField, (*message.Message).GetHostname)},
	} {
		if f.name != "" {
			conf.fieldMap[f.name] = f.fn
		}
	}
}

func (conf *JSONEncoderConfig) encodeStringField(fieldName string, getter func(*message.Message) string) fieldEncoder {
	return func(rawMap map[string]interface{}, msg *message.Message) {
		if val := getter(msg); val != "" {
			rawMap[fieldName] = val
		}
	}
}

func init() {
	pipeline.RegisterPlugin("JSONEncoder", func() interface{} { return new(JSONEncoder) })
}
