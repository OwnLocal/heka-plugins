package hekalocal_test

import (
	"bytes"
	"encoding/json"
	"sort"
	"testing"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
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

func (dt *decoderTester) testDecodeError(payload string, errorMatcher types.GomegaMatcher) error {
	// Set up the pack and run the decoder.
	dt.pack = &pipeline.PipelinePack{}
	dt.pack.Message = &message.Message{Payload: &payload}
	packs, err := dt.decoder.Decode(dt.pack)

	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	val, ok := packs[0].Message.GetFieldValue("decode_error")
	gomega.Expect(ok).To(gomega.BeTrue())
	gomega.Expect(val).To(errorMatcher)

	val, ok = packs[0].Message.GetFieldValue("payload")
	gomega.Expect(ok).To(gomega.BeTrue())
	gomega.Expect(val).To(gomega.Equal(payload))

	return err
}

type encoderTester struct {
	t       *testing.T
	encoder pipeline.Encoder
}

func newEncoderTester(t *testing.T, encoder pipeline.Encoder, encoderConfig interface{}) *encoderTester {
	gomega.RegisterTestingT(t)
	et := &encoderTester{
		t:       t,
		encoder: encoder,
	}
	encoder.(pipeline.Plugin).Init(encoderConfig)

	return et
}

func (et *encoderTester) doEncode(msg *message.Message) ([]byte, error) {
	// Set up the pack and run the encoder.
	pack := &pipeline.PipelinePack{Message: msg}
	return et.encoder.Encode(pack)
}

func (et *encoderTester) testEncode(msg *message.Message, expectedJSON string) {
	encoded, err := et.doEncode(msg)

	if err != nil {
		et.t.Error(err)
	}

	gomega.Expect(encoded).To(gomega.MatchJSON(expectedJSON))
}

func compactJSON(src []byte) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, src); err != nil {
		panic("Failed compacting JSON")
	}
	return buf.Bytes()
}
