package hekalocal_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"

	"github.com/OwnLocal/heka-plugins"
	"github.com/mozilla-services/heka/message"
	"github.com/onsi/gomega"
)

func TestEncode(t *testing.T) {
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

	et := newEncoderTester(t, &hekalocal.JSONEncoder{}, &hekalocal.JSONEncoderConfig{})
	for _, c := range cases {
		et.testEncode(&message.Message{Fields: c.in}, c.want)
	}
}

func intPtr(i int64) *int64 {
	return &i
}

func TestEncodeTimestamp(t *testing.T) {
	cases := []struct {
		in       *int64
		wantJSON string
	}{
		{intPtr(time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC).UnixNano()), `{"@timestamp": "2015-10-10T10:10:10Z"}`},
		{nil, `{}`},
	}

	et := newEncoderTester(t, &hekalocal.JSONEncoder{}, &hekalocal.JSONEncoderConfig{TimestampField: "@timestamp"})
	for _, c := range cases {
		et.testEncode(&message.Message{Timestamp: c.in}, c.wantJSON)
	}
}

func TestEncodeUUID(t *testing.T) {
	cases := []struct {
		in       uuid.UUID
		wantJSON string
	}{
		{uuid.Parse("da8f5b03-5ece-4e45-aad2-0bfa9b99f921"), `{"@uuid": "da8f5b03-5ece-4e45-aad2-0bfa9b99f921"}`},
		{nil, `{}`},
	}

	et := newEncoderTester(t, &hekalocal.JSONEncoder{}, &hekalocal.JSONEncoderConfig{UUIDField: "@uuid"})
	for _, c := range cases {
		et.testEncode(&message.Message{Uuid: c.in}, c.wantJSON)
	}
}

func TestEncodeSeverity(t *testing.T) {
	cases := []struct {
		in       int32
		wantJSON string
	}{
		{0, `{"severity": 0}`},
		{1, `{"severity": 1}`},
		{2, `{"severity": 2}`},
		{7, `{"severity": 7}`},
		{53, `{"severity": 53}`},
	}

	et := newEncoderTester(t, &hekalocal.JSONEncoder{}, &hekalocal.JSONEncoderConfig{SeverityField: "severity"})
	for _, c := range cases {
		et.testEncode(&message.Message{Severity: &c.in}, c.wantJSON)
	}
}

func TestEncodeSeverityDefault(t *testing.T) {
	et := newEncoderTester(t, &hekalocal.JSONEncoder{}, &hekalocal.JSONEncoderConfig{SeverityField: "severity"})
	et.testEncode(&message.Message{}, `{"severity": 7}`)
}

func TestEncodeStringFields(t *testing.T) {
	conf := hekalocal.JSONEncoderConfig{}

	for _, f := range []struct {
		name     string
		field    *string
		setField func(*message.Message, string)
	}{
		{"type", &conf.TypeField, (*message.Message).SetType},
		{"logger", &conf.LoggerField, (*message.Message).SetLogger},
		{"env_version", &conf.EnvVersionField, (*message.Message).SetEnvVersion},
		{"hostname", &conf.HostnameField, (*message.Message).SetHostname},
	} {
		*f.field = f.name
		et := newEncoderTester(t, &hekalocal.JSONEncoder{}, &conf)

		msg := &message.Message{}
		f.setField(msg, "string value!")
		et.testEncode(msg, fmt.Sprintf(`{"%s": "string value!"}`, f.name))

		*f.field = ""
	}
}

func int32Ptr(i int32) *int32 {
	return &i
}

func TestEncodePID(t *testing.T) {
	cases := []struct {
		in       *int32
		wantJSON string
	}{
		{int32Ptr(0), `{"pid": 0}`},
		{int32Ptr(1), `{"pid": 1}`},
		{int32Ptr(2), `{"pid": 2}`},
		{int32Ptr(7), `{"pid": 7}`},
		{int32Ptr(53), `{"pid": 53}`},
		{nil, `{}`},
	}

	et := newEncoderTester(t, &hekalocal.JSONEncoder{}, &hekalocal.JSONEncoderConfig{PIDField: "pid"})
	for _, c := range cases {
		et.testEncode(&message.Message{Pid: c.in}, c.wantJSON)
	}
}

func TestAddBulkHeader(t *testing.T) {
	conf := &hekalocal.JSONEncoderConfig{
		ElasticsearchBulk:  true,
		ElasticsearchIndex: "heka-%{2006.01}",
		ElasticsearchType:  "%{Type}",
		ElasticsearchID:    "%{UUID}",
	}
	et := newEncoderTester(t, &hekalocal.JSONEncoder{}, conf)
	msg := &message.Message{}
	msg.SetType("test_log")
	msg.SetTimestamp(time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC).UnixNano())
	msg.SetUuid(uuid.Parse("de305d54-75b4-431b-adb2-eb6b9e546014"))
	message.NewStringField(msg, "foo", "bar")

	encoded, err := et.doEncode(msg)
	if err != nil {
		et.t.Error(err)
	}
	parts := bytes.Split(encoded, []byte("\n"))
	gomega.Expect(parts[0]).To(gomega.MatchJSON(`{"index": {"_index": "heka-2015.10", "_type": "test_log", "_id":"de305d54-75b4-431b-adb2-eb6b9e546014"}}`))
	gomega.Expect(parts[1]).To(gomega.MatchJSON(`{"foo": "bar"}`))
}
