package pb

import (
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// OffsetReader defines a behavior needed for decoding, i.e. reading bytes starting at a particular offset.
// For example, files implement this interface via their ReadAt method.
type OffsetReader interface {
	// ReadAt reads from a source starting from offset into dest.
	// To restrict the number of bytes read into dest, make dest a certain length.
	// It returns the number of bytes read or an error. If an error is returned,
	// you cannot rely on the data read (or not read) into dest.
	ReadAt(dest []byte, offset int64) (int, error)
}

// Decode reads from r starting at offset. It first gets the little endian encoded length
// of the Entry data, and then reads the log entry's bytes. Once read it unmarshals them
// into a log entry, which it returns along with the number of bytes read [4 + len(log-entry-bytes)] unless an error occurs.
func Decode(r OffsetReader, offset int64) (*Entry, int, error) {
	// read the bytes storing the size of the data
	sb := make([]byte, 4) // stored as uint32 (4 bytes)
	_, err := r.ReadAt(sb, offset)
	if err != nil {
		return nil, 0, err
	}

	// convert the bytes to a uint32
	sz := binary.LittleEndian.Uint32(sb)

	// read the log entry bytes
	data := make([]byte, sz)
	_, err = r.ReadAt(data, offset+4) // add 4 bytes since we read uint32 size already
	if err != nil {
		return nil, 0, err // keep error as is so caller can detect io.EOF
	}

	// unmarshal the data bytes into an Entry instance
	var entry Entry
	err = proto.Unmarshal(data, &entry)
	if err != nil {
		return nil, 0, errors.Wrap(err, "could not unmarshal bytes")
	}

	return &entry, 4 + len(data), nil
}

// DecodeInfo reads from r starting at offset. It first gets the little endian encoded length
// of the Entry data, and then reads the segment info's bytes. Once read it unmarshals them
// into a segment info object, which it returns along with the number of bytes read [4 + len(log-entry-bytes)] unless an error occurs.
func DecodeInfo(r OffsetReader, offset int64) (*SegmentInfo, int64, error) {
	// read the bytes storing the size of the data
	sb := make([]byte, 4) // stored as uint32 (4 bytes)
	_, err := r.ReadAt(sb, offset)
	if err != nil {
		return nil, 0, err
	}

	// convert the bytes to a uint32
	sz := binary.LittleEndian.Uint32(sb)

	// read the segment info bytes
	data := make([]byte, sz)
	_, err = r.ReadAt(data, offset+4) // add 4 bytes since we read uint32 size already
	if err != nil {
		return nil, 0, err // keep error as is so caller can detect io.EOF
	}

	// unmarshal the data bytes into a SegmentInfo instance
	var info SegmentInfo
	err = proto.Unmarshal(data, &info)
	if err != nil {
		return nil, 0, errors.Wrap(err, "could not unmarshal bytes")
	}

	return &info, int64(4 + len(data)), nil
}
