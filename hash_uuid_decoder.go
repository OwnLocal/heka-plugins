package hekalocal

import (
	"crypto/md5"
	"time"

	"github.com/mozilla-services/heka/pipeline"
)

// HashUUIDDecoder sets the UUID to a hashed combination of the timestamp and payload.
type HashUUIDDecoder struct {
	Timestamp time.Time // Hard-code the timestamp for testing
}

// Init is provided to make HashUUIDDecoder implement the Heka pipeline.Plugin interface.
func (d *HashUUIDDecoder) Init(config interface{}) (err error) {
	return
}

// Decode is provided to make HashUUIDDecoder implement the Heka pipeline.Decoder interface.
func (d *HashUUIDDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack, err error) {
	ts := pack.Message.GetTimestamp()
	if !d.Timestamp.IsZero() {
		ts = d.Timestamp.UnixNano()
	}
	hash := md5.Sum([]byte(pack.Message.GetPayload()))
	pack.Message.SetUuid([]byte(NewTimestampUUID(ts, hash[0:])))
	packs = append(packs, pack)
	return
}

func init() {
	pipeline.RegisterPlugin("HashUUIDDecoder", func() interface{} { return new(HashUUIDDecoder) })
}
