package hekalocal

import (
	"encoding/json"
	"strings"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

// UnflattenDecoder converts from fields with dotted names as keys to nested JSON-encoded objects.
// Currently only supports keys with single dots.
type UnflattenDecoder struct{}

// Init is provided to make UnflattenDecoder implement the Heka pipeline.Plugin interface.
func (d *UnflattenDecoder) Init(config interface{}) (err error) {
	return
}

// Decode is provided to make UnflattenDecoder implement the Heka pipeline.Decoder interface.
func (d *UnflattenDecoder) Decode(pack *pipeline.PipelinePack) ([]*pipeline.PipelinePack, error) {
	var newFields []*message.Field
	unflat := map[string]map[string]interface{}{}
	for _, field := range pack.Message.Fields {
		parts := strings.SplitN(field.GetName(), ".", 2)
		if len(parts) < 2 {
			newFields = append(newFields, field)
			continue
		}

		m, exists := unflat[parts[0]]
		if !exists {
			m = map[string]interface{}{}
			unflat[parts[0]] = m
		}
		m[parts[1]] = field.GetValue()
	}
	for k, v := range unflat {
		enc, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		field, err := message.NewField(k, enc, "json")
		if err != nil {
			return nil, err
		}
		newFields = append(newFields, field)
	}
	pack.Message.Fields = newFields
	return []*pipeline.PipelinePack{pack}, nil
}

func init() {
	pipeline.RegisterPlugin("UnflattenDecoder", func() interface{} { return new(UnflattenDecoder) })
}
