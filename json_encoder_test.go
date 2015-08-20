package hekalocal_test

import (
	"testing"
	"time"

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

	encoder := hekalocal.JSONEncoder{}
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

func TestEncodeTimestamp(t *testing.T) {
	RegisterTestingT(t)

	cases := []struct {
		in       int64
		wantJSON string
	}{
		{time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC).UnixNano(), `{"@timestamp": "2015-10-10T10:10:10Z"}`},
	}

	for _, c := range cases {
		enc := hekalocal.JSONEncoder{}
		conf := &hekalocal.JSONEncoderConfig{TimestampField: "@timestamp"}
		enc.Init(conf)
		pack := &pipeline.PipelinePack{}
		pack.Message = &message.Message{Timestamp: &c.in}

		out, err := enc.Encode(pack)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(out)).To(MatchJSON(c.wantJSON))
	}
}
