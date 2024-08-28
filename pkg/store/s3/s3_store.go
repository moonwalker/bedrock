package s3store

import (
	"bytes"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/moonwalker/bedrock/pkg/store"
)

const (
	bucketRegion = "eu-central-1"
)

type s3store struct {
	bucketName string

	s3 *s3.S3
}

func New(bucketName string) store.Store {
	return &s3store{bucketName: bucketName}
}

func (s *s3store) open() (err error) {
	s.s3 = s3.New(session.New())

	inp := &s3.CreateBucketInput{
		Bucket: aws.String(s.bucketName),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(bucketRegion),
		},
	}

	_, err = s.s3.CreateBucket(inp)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			// case s3.ErrCodeBucketAlreadyExists:
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				err = nil
			}
		}
	}

	return
}

func (s *s3store) GetInternalStore() interface{} {
	return nil // not implemented yet
}

func (s *s3store) Get(key string) (val []byte, err error) {
	inp := &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
	}

	err = s.open()
	if err != nil {
		return
	}

	out, err := s.s3.GetObject(inp)
	if err != nil {
		return
	}

	val, err = io.ReadAll(out.Body)
	return
}

func (s *s3store) Set(key string, val []byte, options *store.WriteOptions) (err error) {
	inp := &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
		ACL:    aws.String("public-read"),
		Body:   aws.ReadSeekCloser(bytes.NewReader(val)),
	}

	if options != nil {
		if len(options.ContentType) > 0 {
			inp.ContentType = aws.String(options.ContentType)
		}
	}

	err = s.open()
	if err != nil {
		return
	}

	out, err := s.s3.PutObject(inp)
	_ = out // to avoid declared and not used error

	return
}

func (s *s3store) Delete(key string) (err error) {
	inp := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
	}

	err = s.open()
	if err != nil {
		return
	}

	out, err := s.s3.DeleteObject(inp)
	_ = out // to avoid declared and not used error

	return
}

func (s *s3store) DeleteAll(pattern string) error {
	return errors.New("Not implemented yet.")
}

func (s *s3store) Exists(key string) (bool, error) {
	err := s.open()
	if err != nil {
		return false, err
	}

	return false, errors.New("Not implemented yet.")
}

func (s *s3store) Expire(key string, ttl int64) error {
	err := s.open()
	if err != nil {
		return err
	}

	return errors.New("Not implemented yet.")
}

func (s *s3store) Scan(prefix string, skip int, limit int, fn func(key string, val []byte)) (err error) {
	inp := &s3.ListObjectsInput{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(prefix),
	}

	err = s.open()
	if err != nil {
		return
	}

	err = s.s3.ListObjectsPages(inp, func(p *s3.ListObjectsOutput, last bool) bool {
		for _, obj := range p.Contents {
			key := *obj.Key

			val, err := s.Get(key)
			if err != nil {
				return false
			}

			fn(key, val)
		}
		return true
	})

	return
}

func (s *s3store) Count(prefix string) int {
	return -1 // not implemented yet
}

func (s *s3store) Close() (err error) {
	return // nothing to do here?
}
