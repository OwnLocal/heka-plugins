package ol_heka_test

import (
	"testing"

	"github.com/OwnLocal/heka-plugins"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	. "github.com/onsi/gomega"
)

func TestEncode(t *testing.T) {
	RegisterTestingT(t)

	cases := []struct {
		in   fields
		want string
	}{
		{fields{newField("s", "a string", "")}, `{"s":"a string"}`},
		{fields{newField("n", 42.0, "")}, `{"n":42}`},
		{fields{newField("n", -42.0, "")}, `{"n":-42}`},
		{fields{newField("t", true, "")}, `{"t":true}`},
		{fields{newField("f", false, "")}, `{"f":false}`},

		{fields{newField("a", []byte("[]"), "json")}, `{"a":[]}`},
		{fields{newField("a", []byte("[1, 2, 3, 4]"), "json")}, `{"a":[1, 2, 3, 4]}`},

		{fields{newField("o", []byte("{}"), "json")}, `{"o":{}}`},
		{fields{newField("o", []byte(`{"a":"b", "c": "d"}`), "json")}, `{"o":{"a":"b", "c": "d"}}`},

		{fields{
			newField("s", "foo", ""),
			newField("n", 42.0, ""),
			newField("b", false, ""),
			newField("o", []byte(`{
                  "a": "b",
                  "c": "d"
                }`), "json"),
		},
			`{
            "s": "foo",
            "n": 42,
            "b": false,
            "o": {
                  "a": "b",
                  "c": "d"
                }
            }`,
		},
	}

	encoder := ol_heka.JsonEncoder{}
	pack := &pipeline.PipelinePack{}

	for _, c := range cases {
		pack.Message = &message.Message{Fields: c.in}
		out, err := encoder.Encode(pack)
		if err != nil {
			t.Error(err)
		}

		Expect(out).To(MatchJSON(c.want))
	}
}
