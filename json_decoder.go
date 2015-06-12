package ol_heka

import (
	"encoding/json"
	"unicode"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type JsonDecoder struct{}

func (jd *JsonDecoder) Init(config interface{}) (err error) {
	return
}

func (jd *JsonDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack, err error) {
	packs = []*pipeline.PipelinePack{pack}
	err = decodeJson(pack.Message.GetPayload(), pack.Message)
	return
}

func decodeJson(jsonStr string, msg *message.Message) (err error) {
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
		msg.AddField(field)
	}
	return
}

//TODO: write tests for this, for a symmetrical JsonEncoder, and write JsonEncoder
//TODO: Add config options for which fields to take Uuid, Timestamp, Type, Logger, Severity, EnvVersion, Pid, Hostname? and also parse those nicely where possible (use ForgivingTimeParse for timestamp)
//TODO: Write Decoder and/or filter that sets UUID based on Hashing fields and then converting to UUID format, using NewHash from go-uuid: http://godoc.org/code.google.com/p/go-uuid/uuid
