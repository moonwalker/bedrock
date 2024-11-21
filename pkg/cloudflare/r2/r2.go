package r2

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/moonwalker/bedrock/pkg/env"
	"github.com/moonwalker/bedrock/pkg/mime"
)

const (
	endpointFmt  = "https://%s.r2.cloudflarestorage.com"
	configRegion = "auto"
)

const (
	VideoKeyFmt = "%s/%s"
)

var (
	accountID       = env.Must("CFL_ACCOUNT_ID")
	bucketName      = env.Must("CFL_R2_BUCKET_NAME")
	domainName      = env.Must("CFL_R2_DOMAIN_NAME")
	accessKeyID     = env.Must("CFL_R2_ACCESS_KEY_ID")
	accessKeySecret = env.Must("CFL_R2_ACCESS_KEY_SECRET")
)

func client() (*s3.Client, error) {
	r2resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: fmt.Sprintf(endpointFmt, accountID),
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(configRegion),
		config.WithEndpointResolverWithOptions(r2resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, accessKeySecret, "")),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg), nil
}

func Upload(key string, body io.Reader) (string, error) {
	client, err := client()
	if err != nil {
		return "", err
	}

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.Concurrency = 1
		u.MaxUploadParts = 1
	})

	mtype, body, err := mime.DetectReader(body)
	if err != nil {
		return "", err
	}

	_, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		ContentType: aws.String(mtype),
		Body:        body,
	})

	if err != nil {
		return "", err
	}

	return FmtURL(key), nil
}

func UploadURL(key string, url string) (string, error) {
	res, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	return Upload(key, res.Body)
}

func FmtURL(key string) string {
	u := &url.URL{
		Host: bucketName + "." + domainName,
		Path: key,
	}
	return u.String()
}
