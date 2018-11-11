package hash

import (
	"reflect"
	"testing"
)

type kv struct {
	key []byte
	val []byte
}

var (
	kvs = []kv{
		kv{
			key: []byte("1"),
			val: []byte("2"),
		},
		kv{
			key: []byte("2"),
			val: []byte("3"),
		},
		kv{
			key: []byte("3"),
			val: []byte("4"),
		},
		kv{
			key: []byte("4"),
			val: []byte("5"),
		},
		kv{
			key: []byte("1"),
			val: []byte("2"),
		},
	}
)

func TestIndexWrite(t *testing.T) {
	runIndexWriteTest(t, NoHash)
	runIndexWriteTest(t, DefaultHash)
}

func runIndexWriteTest(t *testing.T, h HashFunc) {
	hi1, err := NewIndex("./data", h)
	defer hi1.cleanup()
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	for _, test := range kvs {
		err = hi1.Write(test.key, test.val)
		if err != nil {
			t.Log(err)
			t.Fail()
		}

		t.Logf("success - write for key: %s", string(test.key))
	}
}

func TestIndexRead(t *testing.T) {
	runIndexReadTest(t, NoHash)
	runIndexReadTest(t, DefaultHash)
}

func runIndexReadTest(t *testing.T, h HashFunc) {
	hi, err := NewIndex("./data", h)
	defer hi.cleanup()
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	// write all the key, val pairs
	for _, test := range kvs {
		err = hi.Write(test.key, test.val)
		if err != nil {
			t.Log(err)
			t.Fail()
		}
	}

	// try to read each of the keys and verify value is what we expect
	for _, test := range kvs {
		val, err := hi.Read(test.key)
		if err != nil {
			t.Log(err)
			t.Fail()
		}

		if !reflect.DeepEqual(test.val, val) {
			t.Logf("failure - expected: %s, but got: %s", string(test.val), string(val))
			t.Fail()
		}

		t.Logf("success - expected: %s got: %s", string(test.val), string(val))
	}
}
