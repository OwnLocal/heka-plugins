package hekalocal_test

import (
	"testing"
	"time"

	"github.com/OwnLocal/heka-plugins"
	. "github.com/onsi/gomega"
)

func TestHashUUIDDecoder(t *testing.T) {
	cases := []struct {
		in       string
		wantUUID string
	}{
		{`{"timestamp": "2015-10-10T10:10:10Z"}`, "16bc6d00-6f37-11e5-804b-7f8b32bc10ae"},
		{`{"timestamp": "2015-10-10T10:10:10Z", "other": "stuff", "here": "too"}`, "16bc6d00-6f37-11e5-800b-7b8f4ee621ac"},
	}

	dt := newDecoderTester(t, &hekalocal.HashUUIDDecoder{Timestamp: time.Date(2015, 10, 10, 10, 10, 10, 0, time.UTC)}, nil)

	for _, c := range cases {
		dt.testDecode(c.in, nil)
		Expect(dt.pack.Message.GetUuidString()).To(Equal(c.wantUUID))
	}
}
