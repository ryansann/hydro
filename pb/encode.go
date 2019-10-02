package pb

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// Encode accepts an Entry and encodes it into a binary representation prepended with the length of the marshaled entry.
// It returns: <size><entry-bytes> where size is a uint32 encoded in little endian form, and <entry-bytes> is the marshaled log entry.
func Encode(entry *Entry) ([]byte, error) {
	// marshal the data into protobuf format
	data, err := proto.Marshal(entry)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal log entry")
	}

	// write the size of the data to a new buffer
	sizebytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizebytes, uint32(len(data)))
	buf := bytes.NewBuffer(sizebytes)

	// write the data to the buffer
	_, err = buf.Write(data)
	if err != nil {
		return nil, errors.Wrap(err, "could not write data to buffer")
	}

	return buf.Bytes(), nil
}

// EncodeInfo encodes info into its binary representation prepended with the length of the marshaled entry.
// It returns <size><info-bytes> where size is a uint32 encoded in little endian form and <info-bytes> is the marshaled segment info.
func EncodeInfo(info *SegmentInfo) ([]byte, error) {
	// marshal the data into protobuf format
	data, err := proto.Marshal(info)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal segment info")
	}

	// write the size of the data to a new buffer
	sizebytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizebytes, uint32(len(data)))
	buf := bytes.NewBuffer(sizebytes)

	// write the data to the buffer
	_, err = buf.Write(data)
	if err != nil {
		return nil, errors.Wrap(err, "could not write data to buffer")
	}

	return buf.Bytes(), nil
}
