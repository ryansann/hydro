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
	hi1, err := New(".", NoHash)
	defer hi1.cleanup()
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	hi2, err := New(".", DefaultHash)
	defer hi2.cleanup()
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

		err = hi2.Write(test.key, test.val)
		if err != nil {
			t.Log(err)
			t.Fail()
		}
	}
}

func TestIndexRead(t *testing.T) {
	hi, err := New(".", NoHash)
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
			t.Logf("fail - expected: %s, but got: %s", string(test.val), string(val))
			t.Fail()
		}

		t.Logf("success - expected: %s got: %s", string(test.val), string(val))
	}
}
