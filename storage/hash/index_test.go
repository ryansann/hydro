package hash

import "testing"

type writeT struct {
	key []byte
	val []byte
}

var (
	writeTests = []writeT{
		writeT{
			key: []byte("1"),
			val: []byte("2"),
		},
		writeT{
			key: []byte("2"),
			val: []byte("3"),
		},
		writeT{
			key: []byte("3"),
			val: []byte("4"),
		},
		writeT{
			key: []byte("4"),
			val: []byte("5"),
		},
		writeT{
			key: []byte("1"),
			val: []byte("2"),
		},
	}
)

func TestHashWrite(t *testing.T) {
	hi1, err := New(".", NoHash)
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	hi2, err := New(".", DefaultHash)
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	for _, test := range writeTests {
		err = hi1.Write(test.key, test.val)
		if err != nil {
			t.Log(err)
			t.Fail()
		}

		err = hi2.Write(test.key, test.val)
		if err != nil {
			t.Log(err)
			t.Fail()
		}
	}

	hi1.cleanup()
	hi2.cleanup()
}
