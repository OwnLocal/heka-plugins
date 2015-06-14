package ol_heka_test

import (
	"sort"
	"testing"

	"github.com/OwnLocal/heka-plugins"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

func TestDecode(t *testing.T) {
	cases := []struct {
		in   string
		want fields
	}{
		{`{"s":"a string"}`, []*message.Field{newField("s", "a string", "")}},
		{`{"n":42}`, fields{newField("n", 42.0, "")}},
		{`{"n":-42}`, fields{newField("n", -42.0, "")}},
		{`{"t":true}`, fields{newField("t", true, "")}},
		{`{"f":false}`, fields{newField("f", false, "")}},

		{`{"a":[]}`, fields{newField("a", []byte("[]"), "json")}},
		{`{"a":[1, 2, 3, 4]}`, fields{newField("a", []byte("[1, 2, 3, 4]"), "json")}},

		{`{"o":{}}`, fields{newField("o", []byte("{}"), "json")}},
		{`{"o":{"a":"b", "c": "d"}}`, fields{newField("o", []byte(`{"a":"b", "c": "d"}`), "json")}},

		{`{
            "s": "foo",
            "n": 42,
            "b": false,
            "o": {
                  "a": "b",
                  "c": "d"
                }
            }`,
			fields{
				newField("s", "foo", ""),
				newField("n", 42.0, ""),
				newField("b", false, ""),
				newField("o", []byte(`{
                  "a": "b",
                  "c": "d"
                }`), "json"),
			},
		},
	}

	decoder := ol_heka.JsonDecoder{}
	pack := &pipeline.PipelinePack{}

	for _, c := range cases {
		pack.Message = &message.Message{Payload: &c.in}
		packs, err := decoder.Decode(pack)
		if err != nil {
			t.Error(err)
		}

		// Sort both sets of fields so they compare properly.
		sort.Sort(fields(packs[0].Message.Fields))
		sort.Sort(c.want)

		if !(&message.Message{Fields: packs[0].Message.Fields}).Equals(&message.Message{Fields: c.want}) {
			t.Errorf("Expected\n%v\ngot\n%v", c.want, packs[0].Message.Fields)
		}
	}
}
