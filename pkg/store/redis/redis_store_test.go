// $ go test -v pkg/store/redis/*.go

package redistore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	store := New("redis://localhost:6379")
	err := store.Set("testkey", []byte("testval"), nil)
	if err != nil {
		t.Error("error setting key/value", err)
	}
}

func TestGet(t *testing.T) {
	store := New("redis://localhost:6379")
	val, err := store.Get("testkey")
	if err != nil {
		t.Error("error getting from store", err)
	}
	if fmt.Sprintf("%s", val) != "testval" {
		t.Error(fmt.Sprintf(`get invalid value. got %s expected "testval"`, val))
	}
}

func TestDelete(t *testing.T) {
	store := New("redis://localhost:6379")
	err := store.Delete("testkey")
	if err != nil {
		t.Error("error deleting from store", err)
	}
	val, _ := store.Get("testkey")
	if val != nil {
		t.Error(fmt.Sprintf("unable to delete. got %s expected nil", val))
	}
}

func TestScan(t *testing.T) {
	store := New("redis://localhost:6379")
	store.DeleteAll("testkey*")
	store.Set("testkey1", []byte("testval1"), nil)
	store.Set("testkey2", []byte("testval2"), nil)
	store.Set("testkey3", []byte("testval3"), nil)
	err := store.Scan("testkey*", 0, 0, func(key string, val []byte) {
		if key == "testkey1" && fmt.Sprintf("%s", val) != "testval1" {
			t.Error(fmt.Sprintf(`get invalid value. got %s expected "testval1"`, val))
		}
		if key == "testkey2" && fmt.Sprintf("%s", val) != "testval2" {
			t.Error(fmt.Sprintf(`get invalid value. got %s expected "testval2"`, val))
		}
		if key == "testkey3" && fmt.Sprintf("%s", val) != "testval3" {
			t.Error(fmt.Sprintf(`get invalid value. got %s expected "testval3"`, val))
		}
	})
	if err != nil {
		t.Error("error scanning the store", err)
	}
}

func TestScanSkipLimit(t *testing.T) {
	store := New("redis://localhost:6379")
	store.DeleteAll("testkey*")
	for i := 0; i < 100; i++ {
		store.Set(fmt.Sprintf("testkey%d", i), []byte(fmt.Sprintf("testval%d", i)), nil)
	}
	c := 0
	err := store.Scan("testkey*", 10, 50, func(key string, val []byte) {
		c++
	})
	assert.Equal(t, 50, c)
	if err != nil {
		t.Error("error scanning the store", err)
	}
}

func TestCount(t *testing.T) {
	store := New("redis://localhost:6379")
	store.DeleteAll("testkey*")
	for i := 0; i < 100; i++ {
		store.Set(fmt.Sprintf("testkey%d", i), []byte(fmt.Sprintf("testval%d", i)), nil)
	}
	c := store.Count("testkey*")
	assert.Equal(t, 100, c)
}

// remove test keys, should be the last func
func TestTeardown(t *testing.T) {
	store := New("redis://localhost:6379")
	store.DeleteAll("testkey*")
}
