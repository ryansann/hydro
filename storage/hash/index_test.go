package hash

import "testing"

func TestHashWrite(t *testing.T) {
	hi, err := New(".", DefaultHash)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	err = hi.Write([]byte("key"), []byte("val"))
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	hi.cleanup()
}
