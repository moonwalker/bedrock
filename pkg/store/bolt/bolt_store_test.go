// $ go test -v pkg/store/bolt/*.go
package boltstore

import (
	"fmt"
	"testing"
)

func TestSet(t *testing.T) {
	t.Log("Set, expect no errors")
	store := New("/tmp/mw-backend-test.db", "mw-backend-test")
	err := store.Set("testkey", []byte("testval"), nil)
	if err != nil {
		t.Error("error setting key/value", err)
	}
	store.Close()
}

func TestGet(t *testing.T) {
	t.Log(`Get, expect "testval"`)
	store := New("/tmp/mw-backend-test.db", "mw-backend-test")
	val, err := store.Get("testkey")
	if err != nil {
		t.Error("error getting from store", err)
	}
	if fmt.Sprintf("%s", val) != "testval" {
		t.Error(fmt.Sprintf(`get invalid value. got %s expected "testval"`, val))
	}
	store.Close()
}

func TestDelete(t *testing.T) {
	t.Log("Delete, expect nil")
	store := New("/tmp/mw-backend-test.db", "mw-backend-test")
	err := store.Delete("testkey")
	if err != nil {
		t.Error("error deleting from store", err)
	}
	val, _ := store.Get("testkey")
	if val != nil {
		t.Error(fmt.Sprintf("unable to delete. got %s expected nil", val))
	}
	store.Close()
}
