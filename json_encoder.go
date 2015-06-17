package ol_heka

import (
	"encoding/json"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type JsonEncoder struct{}

func (enc *JsonEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	rawMap := make(map[string]interface{})
	for _, field := range pack.Message.GetFields() {
		if field.GetValueType() == message.Field_BYTES && field.GetRepresentation() == "json" {
			rawMap[field.GetName()] = (*json.RawMessage)(&field.GetValueBytes()[0])
		} else {
			rawMap[field.GetName()] = field.GetValue()
		}
	}
	output, err = json.Marshal(rawMap)
	return
}

func init() {
	pipeline.RegisterPlugin("JsonEncoder", func() interface{} { return new(JsonEncoder) })
}
