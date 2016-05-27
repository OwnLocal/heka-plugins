package hekalocal

import (
	"encoding/binary"

	"github.com/pborman/uuid"
)

// Most of the code in this file is borrowed from the go-uuid project.

const (
	lillian    = 2299160          // Julian day of 15 Oct 1582
	unix       = 2440587          // Julian day of 1 Jan 1970
	epoch      = unix - lillian   // Days between epochs
	g1582      = epoch * 86400    // seconds between epochs
	g1582ns100 = g1582 * 10000000 // 100s of a nanoseconds between epochs
)

// NewTimestampUUID returns a modified UUID based on Version 1 UUID with the specified timestamp as
// the timestamp portion and the provided bytes (assumed to be a hashed version of the represented
// data) copied into the rest of the UUID bytes.
func NewTimestampUUID(timestamp int64, hash []byte) uuid.UUID {
	t := uint64(timestamp/100) + g1582ns100

	uuid := make([]byte, 16)

	time_low := uint32(t & 0xffffffff)
	time_mid := uint16((t >> 32) & 0xffff)
	time_hi := uint16((t >> 48) & 0x0fff)
	time_hi |= 0x1000 // Version 1

	binary.BigEndian.PutUint32(uuid[0:], time_low)
	binary.BigEndian.PutUint16(uuid[4:], time_mid)
	binary.BigEndian.PutUint16(uuid[6:], time_hi)
	uuid[8] = 0x80 // Specify it is a RFC4122 UUID
	copy(uuid[9:], hash)

	return uuid
}
