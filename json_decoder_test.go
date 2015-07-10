package hekalocal_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/OwnLocal/heka-plugins"
	"github.com/mozilla-services/heka/message"
	. "github.com/onsi/gomega"
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

	dt := newDecoderTester(t, &hekalocal.JSONDecoder{}, &hekalocal.JSONDecoderConfig{})

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

	dt := newDecoderTester(t, &hekalocal.JSONDecoder{}, &hekalocal.JSONDecoderConfig{TimestampField: "@timestamp"})

	for _, c := range cases {
		dt.testDecode(c.in, c.wantFields)
		Expect(dt.pack.Message.GetTimestamp()).To(Equal(c.wantTimestamp))
	}
}

func TestDecodeBadTimestamp(t *testing.T) {
	cases := []interface{}{
		"2015-10T10:10:10Z",
		"Not even close",
	}

	dt := newDecoderTester(t, &hekalocal.JSONDecoder{}, &hekalocal.JSONDecoderConfig{TimestampField: "@timestamp"})

	for _, c := range cases {
		Expect(dt.testDecodeError(fmt.Sprintf(`{"@timestamp": %#v}`, c))).To(HaveOccurred())
	}
}

func TestDecodeUUID(t *testing.T) {
	cases := []struct {
		in         string
		wantUUID   string
		wantFields fields
	}{
		{`{"NotUuid": "8fa6b692-5696-41f5-a0ba-a32f9c6d8d6d"}`, "", fields{newField("NotUuid", "8fa6b692-5696-41f5-a0ba-a32f9c6d8d6d", "")}},
		{`{"@uuid": "8fa6b692-5696-41f5-a0ba-a32f9c6d8d6d"}`, "8fa6b692-5696-41f5-a0ba-a32f9c6d8d6d", nil},
	}

	dt := newDecoderTester(t, &hekalocal.JSONDecoder{}, &hekalocal.JSONDecoderConfig{UUIDField: "@uuid"})

	for _, c := range cases {
		dt.testDecode(c.in, c.wantFields)
		Expect(dt.pack.Message.GetUuidString()).To(Equal(c.wantUUID))
	}
}

func TestDecodeBadUUID(t *testing.T) {
	cases := []string{
		`{"@uuid": "8fa6b692-5696-41f5-a0ba"}`,
		`{"@uuid": 42}`,
		`{"@uuid": false}`,
		`{"@uuid": null}`,
	}

	dt := newDecoderTester(t, &hekalocal.JSONDecoder{}, &hekalocal.JSONDecoderConfig{UUIDField: "@uuid"})

	for _, c := range cases {
		Expect(dt.testDecodeError(c)).To(MatchError(ContainSubstring("Not a valid UUID")))
	}
}

func TestDecodeSeverity(t *testing.T) {
	cases := []struct {
		in        interface{}
		wantLevel int32
	}{
		{"emerg", 0}, {"EMERGENCY", 0},
		{"alert", 1}, {"ALERT", 1}, {"A", 1},
		{"crit", 2}, {"CRITICAL", 2}, {"C", 2},
		{"err", 3}, {"ERROR", 3}, {"E", 3},
		{"warning", 4}, {"WARN", 4}, {"W", 4},
		{"notice", 5}, {"NOTICE", 5}, {"N", 5},
		{"info", 6}, {"INFORMATION", 6}, {"I", 6},
		{"debug", 7}, {"DEBUG", 7}, {"D", 7},
		{42, 42},
		{"Not a valid thing", 7},
	}

	dt := newDecoderTester(t, &hekalocal.JSONDecoder{}, &hekalocal.JSONDecoderConfig{SeverityField: "severity"})
	for _, c := range cases {
		dt.testDecode(fmt.Sprintf(`{"severity": %#v}`, c.in), nil)
		Expect(dt.pack.Message.GetSeverity()).To(Equal(c.wantLevel))
	}
}

func TestDecodeStringFields(t *testing.T) {
	conf := hekalocal.JSONDecoderConfig{}

	for _, f := range []struct {
		name     string
		field    *string
		getField func(*message.Message) string
	}{
		{"type", &conf.TypeField, (*message.Message).GetType},
		{"logger", &conf.LoggerField, (*message.Message).GetLogger},
		{"env_version", &conf.EnvVersionField, (*message.Message).GetEnvVersion},
		{"hostname", &conf.HostnameField, (*message.Message).GetHostname},
	} {
		*f.field = f.name
		dt := newDecoderTester(t, &hekalocal.JSONDecoder{}, &conf)

		cases := []struct {
			in         string
			wantVal    string
			wantFields fields
		}{
			{`{"NotField": "not-val"}`, "", fields{newField("NotField", "not-val", "")}},
			{fmt.Sprintf(`{"%s": "good-val"}`, f.name), "good-val", nil},
			{fmt.Sprintf(`{"%s": 42}`, f.name), "", nil},
		}

		for _, c := range cases {
			dt.testDecode(c.in, c.wantFields)
			Expect(f.getField(dt.pack.Message)).To(Equal(c.wantVal))
		}

		*f.field = ""
	}
}

func TestDecodeIntFields(t *testing.T) {
	conf := hekalocal.JSONDecoderConfig{}

	for _, f := range []struct {
		name       string
		field      *string
		getField   func(*message.Message) int32
		defaultVal int32
	}{
		{"pid", &conf.PIDField, (*message.Message).GetPid, 0},
	} {
		*f.field = f.name
		dt := newDecoderTester(t, &hekalocal.JSONDecoder{}, &conf)

		cases := []struct {
			in         string
			wantVal    int32
			wantFields fields
		}{
			{`{"NotField": 1234}`, f.defaultVal, fields{newField("NotField", 1234.0, "")}},
			{fmt.Sprintf(`{"%s": 1234}`, f.name), 1234, nil},
			{fmt.Sprintf(`{"%s": "foo"}`, f.name), f.defaultVal, nil},
		}

		for _, c := range cases {
			dt.testDecode(c.in, c.wantFields)
			Expect(f.getField(dt.pack.Message)).To(Equal(c.wantVal))
		}

		*f.field = ""
	}
}
