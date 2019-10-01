package hash

//import (
//	"reflect"
//	"testing"
//)
//
//type kv struct {
//	key []byte
//	val []byte
//}
//
//var (
//	kvs = []kv{
//		kv{
//			key: []byte("1"),
//			val: []byte("2"),
//		},
//		kv{
//			key: []byte("2"),
//			val: []byte("3"),
//		},
//		kv{
//			key: []byte("3"),
//			val: []byte("4"),
//		},
//		kv{
//			key: []byte("4"),
//			val: []byte("5"),
//		},
//		kv{
//			key: []byte("1"),
//			val: []byte("2"),
//		},
//	}
//)
//
//func TestIndexWrite(t *testing.T) {
//	t.Run("nohash", func(t *testing.T) {
//		runIndexWriteTest(t, NoHash)
//	})
//	t.Run("defaulthash", func(t *testing.T) {
//		runIndexWriteTest(t, DefaultHash)
//	})
//}
//
//func runIndexWriteTest(t *testing.T, h HashFunc) {
//	hi1, err := NewIndex(SetHashFunc(h))
//	defer hi1.cleanup()
//	if err != nil {
//		t.Log(err)
//		t.Fail()
//		return
//	}
//
//	for _, test := range kvs {
//		err = hi1.Write(test.key, test.val)
//		if err != nil {
//			t.Log(err)
//			t.Fail()
//			return
//		}
//
//		t.Logf("success - write for key: %s", string(test.key))
//	}
//}
//
//func TestIndexRead(t *testing.T) {
//	t.Run("nohash", func(t *testing.T) {
//		runIndexReadTest(t, NoHash)
//	})
//	t.Run("defaulthash", func(t *testing.T) {
//		runIndexReadTest(t, DefaultHash)
//	})
//}
//
//func runIndexReadTest(t *testing.T, h HashFunc) {
//	hi, err := NewIndex(SetHashFunc(h))
//	defer hi.cleanup()
//	if err != nil {
//		t.Log(err)
//		t.Fail()
//		return
//	}
//
//	// write all the key, val pairs
//	for _, test := range kvs {
//		err = hi.Write(test.key, test.val)
//		if err != nil {
//			t.Log(err)
//			t.Fail()
//			return
//		}
//	}
//
//	// try to read each of the keys and verify value is what we expect
//	for _, test := range kvs {
//		val, err := hi.Read(test.key)
//		if err != nil {
//			t.Log(err)
//			t.Fail()
//			return
//		}
//
//		if !reflect.DeepEqual(test.val, val) {
//			t.Logf("failure - expected: %s, but got: %s", string(test.val), string(val))
//			t.Fail()
//			return
//		}
//
//		t.Logf("success - expected: %s got: %s", string(test.val), string(val))
//	}
//}
//
//func TestIndexDelete(t *testing.T) {
//	t.Run("nohash", func(t *testing.T) {
//		runIndexDeleteTest(t, NoHash)
//	})
//	t.Run("defaulthash", func(t *testing.T) {
//		runIndexDeleteTest(t, DefaultHash)
//	})
//}
//
//func runIndexDeleteTest(t *testing.T, h HashFunc) {
//	hi, err := NewIndex(SetHashFunc(h))
//	defer hi.cleanup()
//	if err != nil {
//		t.Log(err)
//		t.Fail()
//		return
//	}
//
//	for _, test := range kvs {
//		err := hi.Write(test.key, test.val)
//		if err != nil {
//			t.Log(err)
//			t.Fail()
//			return
//		}
//
//		err = hi.Delete(test.key)
//		if err != nil {
//			t.Log(err)
//			t.Fail()
//			return
//		}
//
//		key, err := h(test.key)
//		if err != nil {
//			t.Log(err)
//			t.Fail()
//			return
//		}
//
//		if _, ok := hi.keymap[key]; ok {
//			t.Logf("expected key: %s to be deleted", string(test.key))
//			t.Fail()
//			return
//		}
//
//		t.Logf("success - key: %s deleted", string(test.key))
//	}
//}
//
//func TestIndexRestore(t *testing.T) {
//	t.Run("nohash", func(t *testing.T) {
//		runIndexRestoreTest(t, NoHash)
//	})
//	t.Run("defaulthash", func(t *testing.T) {
//		runIndexRestoreTest(t, DefaultHash)
//	})
//}
//
//func runIndexRestoreTest(t *testing.T, h HashFunc) {
//	hi, err := NewIndex(SetHashFunc(h))
//	defer hi.cleanup()
//	if err != nil {
//		t.Log(err)
//		t.Fail()
//		return
//	}
//
//	var lastkey []byte
//
//	// write all the kvs
//	for _, test := range kvs {
//		err := hi.Write(test.key, test.val)
//		if err != nil {
//			t.Log(err)
//			t.Fail()
//			return
//		}
//
//		lastkey = test.key
//	}
//
//	// we should have a deletion operation not just writes
//	err = hi.Delete(lastkey)
//	if err != nil {
//		t.Log(err)
//		t.Fail()
//		return
//	}
//
//	hi.offset = 0
//	hi.keymap = make(map[string]int64, 0)
//
//	err = hi.Restore()
//	if err != nil {
//		t.Log(err)
//		t.Fail()
//		return
//	}
//
//	if len(hi.keymap) == 0 {
//		t.Log("fail - keymap was not restored, length is 0")
//		t.Fail()
//		return
//	}
//
//	t.Log("success - keymap restored")
//}
//
//// We can do 100,000+ writes a second - 9,730 ns/op
//func BenchmarkIndexWrite(b *testing.B) {
//	// set b.N explicitly - we don't want to claim too much disk space (e.g. b = 10,000,000)
//	b.Run("nohash", func(b *testing.B) {
//		b.N = 100000
//		runBenchIndexWrite(b, NoHash)
//	})
//	b.Run("defaulthash", func(b *testing.B) {
//		b.N = 100000
//		runBenchIndexWrite(b, DefaultHash)
//	})
//}
//
//func runBenchIndexWrite(b *testing.B, h HashFunc) {
//	k, v := []byte("key"), []byte("val")
//
//	hi, err := NewIndex(SetHashFunc(h))
//	defer hi.cleanup()
//
//	if err != nil {
//		b.Log(err)
//		b.Fail()
//		return
//	}
//
//	for n := 0; n < b.N; n++ {
//		err := hi.Write(k, v)
//		if err != nil {
//			b.Log(err)
//			b.Fail()
//			return
//		}
//	}
//}
