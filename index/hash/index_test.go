package hash

import (
	"reflect"
	"testing"

	"github.com/ryansann/hydro/storage/file"
)

// NOTE: Should remove the dependency on storage/file here

const (
	storageFile = "./data"
)

type kv struct {
	key string
	val string
}

var (
	kvs = []kv{
		kv{
			key: "1",
			val: "2",
		},
		kv{
			key: "2",
			val: "3",
		},
		kv{
			key: "3",
			val: "4",
		},
		kv{
			key: "4",
			val: "5",
		},
		kv{
			key: "1",
			val: "2",
		},
	}
)

func TestIndexWrite(t *testing.T) {
	t.Run("nohash", func(t *testing.T) {
		runIndexSetTest(t, NoHash)
	})
	t.Run("defaulthash", func(t *testing.T) {
		runIndexSetTest(t, DefaultHash)
	})
}

func runIndexSetTest(t *testing.T, h KeyHashFunc) {
	s, err := file.NewStore(storageFile)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := s.Cleanup()
		if err != nil {
			t.Error(err)
		}
	}()

	i, err := NewIndex(s, SetHashFunc(h))
	if err != nil {
		t.Error(err)
	}

	for _, test := range kvs {
		err = i.Set(test.key, test.val)
		if err != nil {
			t.Error(err)
		}

		t.Logf("success - set key: %s", test.key)
	}
}

func TestIndexGet(t *testing.T) {
	t.Run("nohash", func(t *testing.T) {
		runIndexGetTest(t, NoHash)
	})
	t.Run("defaulthash", func(t *testing.T) {
		runIndexGetTest(t, DefaultHash)
	})
}

func runIndexGetTest(t *testing.T, h KeyHashFunc) {
	s, err := file.NewStore(storageFile)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := s.Cleanup()
		if err != nil {
			t.Error(err)
		}
	}()

	i, err := NewIndex(s, SetHashFunc(h))
	if err != nil {
		t.Error(err)
	}

	// write all the key, val pairs
	for _, test := range kvs {
		err = i.Set(test.key, test.val)
		if err != nil {
			t.Error(err)
		}
	}

	// try to read each of the keys and verify value is what we expect
	for _, test := range kvs {
		val, err := i.Get(test.key)
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(test.val, val) {
			t.Errorf("failure - expected: %s, but got: %s", test.val, val)
		}

		t.Logf("success - expected: %s got: %s", test.val, val)
	}
}

func TestIndexDelete(t *testing.T) {
	t.Run("nohash", func(t *testing.T) {
		runIndexDeleteTest(t, NoHash)
	})
	t.Run("defaulthash", func(t *testing.T) {
		runIndexDeleteTest(t, DefaultHash)
	})
}

func runIndexDeleteTest(t *testing.T, h KeyHashFunc) {
	s, err := file.NewStore(storageFile)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := s.Cleanup()
		if err != nil {
			t.Error(err)
		}
	}()

	i, err := NewIndex(s, SetHashFunc(h))
	if err != nil {
		t.Error(err)
	}

	for _, test := range kvs {
		err := i.Set(test.key, test.val)
		if err != nil {
			t.Error(err)
		}

		err = i.Del(test.key)
		if err != nil {
			t.Error(err)
		}

		key, err := h([]byte(test.key))
		if err != nil {
			t.Error(err)
		}

		if _, ok := i.keys[key]; ok {
			t.Errorf("expected key: %s to be deleted", test.key)
		}

		t.Logf("success - key: %s deleted", test.key)
	}
}

func TestIndexRestore(t *testing.T) {
	t.Run("nohash", func(t *testing.T) {
		runIndexRestoreTest(t, NoHash)
	})
	t.Run("defaulthash", func(t *testing.T) {
		runIndexRestoreTest(t, DefaultHash)
	})
}

func runIndexRestoreTest(t *testing.T, h KeyHashFunc) {
	s, err := file.NewStore(storageFile)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := s.Cleanup()
		if err != nil {
			t.Error(err)
		}
	}()

	i, err := NewIndex(s, SetHashFunc(h))
	if err != nil {
		t.Error(err)
	}

	var lastkey string

	// write all the kvs
	for _, test := range kvs {
		err := i.Set(test.key, test.val)
		if err != nil {
			t.Error(err)
		}

		lastkey = test.key
	}

	// we should have a deletion operation not just writes
	err = i.Del(lastkey)
	if err != nil {
		t.Error(err)
	}

	i.keys = make(map[string]entryLocation, 0)
	err = i.Restore()
	if err != nil {
		t.Error(err)
	}

	if len(i.keys) == 0 {
		t.Error("fail - keys was not restored, length is 0")
	}

	t.Log("success - keys restored")
}

// We can do 100,000+ writes a second - 9,730 ns/op
func BenchmarkIndexWrite(b *testing.B) {
	// set b.N explicitly - we don't want to claim too much disk space (e.g. b = 10,000,000)
	b.Run("nohash", func(b *testing.B) {
		b.N = 100000
		runBenchIndexWrite(b, NoHash)
	})
	b.Run("defaulthash", func(b *testing.B) {
		b.N = 100000
		runBenchIndexWrite(b, DefaultHash)
	})
}

func runBenchIndexWrite(b *testing.B, h KeyHashFunc) {
	k, v := "key", "val"

	s, err := file.NewStore(storageFile)
	if err != nil {
		b.Error(err)
	}
	defer func() {
		err := s.Cleanup()
		if err != nil {
			b.Error(err)
		}
	}()

	i, err := NewIndex(s, SetHashFunc(h))
	if err != nil {
		b.Error(err)
	}

	if err != nil {
		b.Error(err)
	}

	for n := 0; n < b.N; n++ {
		err := i.Set(k, v)
		if err != nil {
			b.Error(err)
		}
	}
}
