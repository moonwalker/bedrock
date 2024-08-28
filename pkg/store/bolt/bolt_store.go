package boltstore

import (
	"bytes"
	"errors"

	"github.com/boltdb/bolt"

	"github.com/moonwalker/bedrock/pkg/store"
)

type boltstore struct {
	storePath  string
	bucketName []byte

	db     *bolt.DB
	opened bool
}

func New(storePath string, bucketName string) store.Store {
	return &boltstore{storePath: storePath, bucketName: []byte(bucketName)}
}

func (s *boltstore) open() (err error) {
	if s.opened {
		return
	}

	s.db, err = bolt.Open(s.storePath, 0600, nil)
	if err != nil {
		return
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(s.bucketName)
		return err
	})

	if err == nil {
		s.opened = true
	}

	return
}

func (s *boltstore) GetInternalStore() interface{} {
	return nil // not implemented yet
}

func (s *boltstore) Get(key string) (val []byte, err error) {
	err = s.open()
	if err != nil {
		return
	}

	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		val = b.Get([]byte(key))
		return nil
	})

	return
}

func (s *boltstore) Set(key string, val []byte, options *store.WriteOptions) (err error) {
	err = s.open()
	if err != nil {
		return
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		return b.Put([]byte(key), val)
	})
}

func (s *boltstore) Exists(key string) (exists bool, err error) {
	err = s.open()
	if err != nil {
		return
	}

	return false, errors.New("Not implemented yet.")
}

func (s *boltstore) Expire(key string, ttl int64) (err error) {
	err = s.open()
	if err != nil {
		return
	}

	return errors.New("Not implemented yet.")
}

func (s *boltstore) Delete(key string) (err error) {
	err = s.open()
	if err != nil {
		return
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		return b.Delete([]byte(key))
	})
}

func (s *boltstore) DeleteAll(pattern string) error {
	return errors.New("Not implemented yet.")
}

func (s *boltstore) Scan(prefix string, skip int, limit int, fn func(key string, val []byte)) (err error) {
	err = s.open()
	if err != nil {
		return
	}

	return s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(s.bucketName).Cursor()
		p := []byte(prefix)
		for k, v := c.Seek(p); k != nil && bytes.HasPrefix(k, p); k, v = c.Next() {
			fn(string(k), v)
		}
		return nil
	})
}

func (s *boltstore) Count(prefix string) int {
	return -1 // not implemented yet
}

func (s *boltstore) Close() error {
	s.opened = false
	return s.db.Close()
}
