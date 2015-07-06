package hekalocal_test

import (
	"sort"
	"testing"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/onsi/gomega"
)

func newField(name string, value interface{}, representation string) *message.Field {
	field, err := message.NewField(name, value, representation)
	if err != nil {
		panic(err)
	}
	return field
}

type fields []*message.Field

// Implement sort.Interface to make fields sortable.
func (f fields) Len() int           { return len(f) }
func (f fields) Less(i, j int) bool { return *(f[i].Name) < *(f[j].Name) }
func (f fields) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

type decoderTester struct {
	t       *testing.T
	decoder pipeline.Decoder
	pack    *pipeline.PipelinePack
}

func newDecoderTester(t *testing.T, decoder pipeline.Decoder, decoderConfig interface{}) *decoderTester {
	gomega.RegisterTestingT(t)
	dt := &decoderTester{
		t:       t,
		decoder: decoder,
	}
	decoder.(pipeline.Plugin).Init(decoderConfig)

	return dt
}

func (dt *decoderTester) testDecode(payload string, expectedFields fields) []*pipeline.PipelinePack {
	// Set up the pack and run the decoder.
	dt.pack = &pipeline.PipelinePack{}
	dt.pack.Message = &message.Message{Payload: &payload}
	packs, err := dt.decoder.Decode(dt.pack)

	if err != nil {
		dt.t.Error(err)
	}

	// Sort both sets of fields so they compare properly.
	sort.Sort(fields(packs[0].Message.Fields))
	sort.Sort(expectedFields)

	// The docs for the Decode method indicate the first pack in its
	// return value should be the pack passed to Decode:
	// http://hekad.readthedocs.org/en/v0.9.2/developing/plugin.html#decoders
	if packs[0] != dt.pack {
		dt.t.Errorf("First pack in Decoder.Decode return value should be the pack passed in.")
	}

	gomega.Expect(packs[0].Message.Fields).To(gomega.Equal([]*message.Field(expectedFields)))

	return packs
}

func (dt *decoderTester) testDecodeError(payload string) error {
	// Set up the pack and run the decoder.
	dt.pack = &pipeline.PipelinePack{}
	dt.pack.Message = &message.Message{Payload: &payload}
	packs, err := dt.decoder.Decode(dt.pack)

	gomega.Expect(packs[0].Message.Fields).To(gomega.BeEmpty())
	return err
}
