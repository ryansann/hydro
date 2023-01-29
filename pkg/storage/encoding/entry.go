package encoding

type Entry interface {
	Key() []byte
	Value() []byte
	Offset() int64
	Timestamp() int64
	Size() int
	Encode() ([]byte, error)
	Decode(data []byte) error
}
