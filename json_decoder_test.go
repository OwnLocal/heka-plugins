package ol_heka_test

import (
	"testing"
	"time"

	"github.com/OwnLocal/heka-plugins"
	"github.com/mozilla-services/heka/message"
	"github.com/onsi/gomega"
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

	dt := newDecoderTester(t, &ol_heka.JsonDecoder{}, &ol_heka.JsonDecoderConfig{})

	for _, c := range cases {
		dt.testDecode(c.in, c.want)
	}
}

func TestDecodeTimestamp(t *testing.T) {
	cases := []struct {
		in            string
		wantTimestamp int64
		wantFields    fields
	}{
		{`{"NotTimestamp": "2015-10-10T10:10:10"}`, 0, fields{newField("NotTimestamp", "2015-10-10T10:10:10", "")}},
		{`{"@timestamp": "2015-10-10T10:10:10Z"}`, time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC).UnixNano(), nil},
		{`{"@timestamp": "2015-10-10T10:10:10.12345Z"}`, time.Date(2015, 10, 10, 10, 10, 10, 123450000, time.UTC).UnixNano(), nil},
		{`{"@timestamp": "2015-10-10T10:10:10Z", "foo": "bar"}`, time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC).UnixNano(), fields{newField("foo", "bar", "")}},
		{`{"@timestamp": 1444471810000000000, "foo": "bar"}`, time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC).UnixNano(), fields{newField("foo", "bar", "")}},
		{`{"@timestamp": 1444471810.0, "foo": "bar"}`, time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC).UnixNano(), fields{newField("foo", "bar", "")}},
		{`{"@timestamp": 1444471810, "foo": "bar"}`, time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC).UnixNano(), fields{newField("foo", "bar", "")}},
		{`{"@timestamp": false, "foo": "bar"}`, 0, fields{newField("foo", "bar", "")}},
		{`{"@timestamp": null, "foo": "bar"}`, 0, fields{newField("foo", "bar", "")}},
	}

	dt := newDecoderTester(t, &ol_heka.JsonDecoder{}, &ol_heka.JsonDecoderConfig{TimestampField: "@timestamp"})

	for _, c := range cases {
		dt.testDecode(c.in, c.wantFields)
		gomega.Expect(dt.pack.Message.GetTimestamp()).To(gomega.Equal(c.wantTimestamp))
	}
}

func TestDecodeUuid(t *testing.T) {
	cases := []struct {
		in         string
		wantUuid   string
		wantFields fields
	}{
		{`{"NotUuid": "8fa6b692-5696-41f5-a0ba-a32f9c6d8d6d"}`, "", fields{newField("NotUuid", "8fa6b692-5696-41f5-a0ba-a32f9c6d8d6d", "")}},
		{`{"@uuid": "8fa6b692-5696-41f5-a0ba-a32f9c6d8d6d"}`, "8fa6b692-5696-41f5-a0ba-a32f9c6d8d6d", nil},
	}

	dt := newDecoderTester(t, &ol_heka.JsonDecoder{}, &ol_heka.JsonDecoderConfig{UuidField: "@uuid"})

	for _, c := range cases {
		dt.testDecode(c.in, c.wantFields)
		gomega.Expect(dt.pack.Message.GetUuidString()).To(gomega.Equal(c.wantUuid))
	}
}

func TestDecodeType(t *testing.T) {
	cases := []struct {
		in         string
		wantType   string
		wantFields fields
	}{
		{`{"NotType": "rails-log"}`, "", fields{newField("NotType", "rails-log", "")}},
		{`{"@type": "rails-log"}`, "rails-log", nil},
	}

	dt := newDecoderTester(t, &ol_heka.JsonDecoder{}, &ol_heka.JsonDecoderConfig{TypeField: "@type"})

	for _, c := range cases {
		dt.testDecode(c.in, c.wantFields)
		gomega.Expect(dt.pack.Message.GetType()).To(gomega.Equal(c.wantType))
	}
}
