package hekalocal_test

import (
	"testing"

	"github.com/OwnLocal/heka-plugins"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	. "github.com/onsi/gomega"
)

func TestUnflattenDecoder(t *testing.T) {
	RegisterTestingT(t)
	cases := []struct {
		in   fields
		want fields
	}{
		{fields{newField("a.b", 42.0, "")}, fields{newField("a", []byte(`{"b":42}`), "json")}},
		{fields{newField("a.b", 42.0, ""), newField("a.d", "foo", "")}, fields{newField("a", []byte(`{"b":42,"d":"foo"}`), "json")}},
		{fields{newField("a", 42.0, "")}, fields{newField("a", 42.0, "")}},
		{fields{newField("a.b", 42.0, ""), newField("c", "d", "")}, fields{newField("c", "d", ""), newField("a", []byte(`{"b":42}`), "json")}},
	}

	d := hekalocal.UnflattenDecoder{}

	for _, c := range cases {
		pack := &pipeline.PipelinePack{}
		pack.Message = &message.Message{Fields: c.in}
		packs, err := d.Decode(pack)
		Expect(err).NotTo(HaveOccurred())
		Expect(packs[0].Message.Fields).To(Equal([]*message.Field(c.want)))
	}
}
