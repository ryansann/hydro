package pb

import (
	"bytes"
	"encoding/binary"
	fmt "fmt"

	proto "github.com/golang/protobuf/proto"
)

// Encode accepts an Entry and encodes it into a binary representation
// prepended with the length of the marshaled entry.
// it returns: <size><entry-bytes> where size is a uint32 encoded in little endian form,
// and <entry-bytes> is the marshaled log entry.
func Encode(entry *Entry) ([]byte, error) {
	// marshal the data into protobuf format
	data, err := proto.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("error: could not marshal log entry: %v", err)
	}

	// write the size of the data to a new buffer
	sizebytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizebytes, uint32(len(data)))
	buf := bytes.NewBuffer(sizebytes)

	// write the data to the buffer
	_, err = buf.Write(data)
	if err != nil {
		return nil, fmt.Errorf("error: could not write data to buffer: %v", err)
	}

	return buf.Bytes(), nil
}
