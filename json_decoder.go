package ol_heka

import (
	"encoding/json"
	"time"
	"unicode"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type JsonDecoder struct {
	config *JsonDecoderConfig
}

type JsonDecoderConfig struct {
	TimestampField string
}

func (jd *JsonDecoder) Init(config interface{}) (err error) {
	jd.config = config.(*JsonDecoderConfig)
	return
}

func (jd *JsonDecoder) ConfigStruct() interface{} {
	return new(JsonDecoderConfig)
}

func (jd *JsonDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack, err error) {
	packs = []*pipeline.PipelinePack{pack}
	err = jd.decodeJson(pack.Message.GetPayload(), pack.Message)
	return
}

func (jd *JsonDecoder) decodeJson(jsonStr string, msg *message.Message) (err error) {
	rawMap := make(map[string]*json.RawMessage)
	if err = json.Unmarshal([]byte(jsonStr), &rawMap); err != nil {
		return
	}

	for key, raw := range rawMap {
		var field *message.Field
		rawS := string(*raw)
		rb := rune(rawS[0])

		// If it's a number, string, or bool, decode it.
		if unicode.IsDigit(rb) || rb == '-' || rb == '"' || rawS == "true" || rawS == "false" {
			var val interface{}
			if err = json.Unmarshal(*raw, &val); err != nil {
				return
			}
			field, err = message.NewField(key, val, "")
		} else {
			// If it's an object, array, or null, leave it as encoded JSON.
			field, err = message.NewField(key, []byte(*raw), "json")
		}
		if err != nil {
			return
		}

		if jd.config.TimestampField != "" && key == jd.config.TimestampField {
			var timestamp time.Time
			timestamp, err = message.ForgivingTimeParse(time.RFC3339, field.GetValueString()[0], time.UTC)
			if err != nil {
				return
			}
			msg.SetTimestamp(timestamp.UnixNano())
			continue
		}

		msg.AddField(field)
	}
	return
}

//TODO: Add config options for which fields to take Uuid, Timestamp, Type, Logger, Severity, EnvVersion, Pid, Hostname? from and also parse those nicely where possible (use ForgivingTimeParse for timestamp)
//TODO: Add config options for encoder on what fields to take from the Heka Message and what fields to put them in in the outgoing JSON
//TODO: Write Decoder and/or filter that sets UUID based on Hashing fields and then converting to UUID format, using NewHash from go-uuid: http://godoc.org/code.google.com/p/go-uuid/uuid

func init() {
	pipeline.RegisterPlugin("JsonDecoder", func() interface{} { return new(JsonDecoder) })
}
