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
	hi1, err := NewIndex()
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
	hi, err := NewIndex()
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

// We can do 100,000+ writes a second - 9,730 ns/op
func BenchmarkIndexWrite(b *testing.B) {
	b.N = 100000 // we don't want to claim too much disk space (e.g. b = 10,000,000)

	runBenchIndexWrite(b, NoHash)
	runBenchIndexWrite(b, DefaultHash)
}

func runBenchIndexWrite(b *testing.B, h HashFunc) {
	k, v := []byte("key"), []byte("val")

	hi, err := NewIndex()
	defer hi.cleanup()

	if err != nil {
		b.Log(err)
		b.Fail()
	}

	for n := 0; n < b.N; n++ {
		err := hi.Write(k, v)
		if err != nil {
			b.Log(err)
			b.Fail()
		}
	}
}
