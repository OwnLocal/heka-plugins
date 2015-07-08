package hekalocal

import (
	"encoding/json"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

// JSONEncoder serializes messages to JSON.
type JSONEncoder struct{}

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
	output, err = json.Marshal(rawMap)
	return
}

func init() {
	pipeline.RegisterPlugin("JSONEncoder", func() interface{} { return new(JSONEncoder) })
}
