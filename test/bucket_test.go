package test

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// go test -v -count 1 -run TestCreateBucketV2Simple ./test
func TestCreateBucketV2Simple(t *testing.T) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.Proxy = http.ProxyFromEnvironment
		tr.TLSClientConfig.InsecureSkipVerify = true
		tr.ExpectContinueTimeout = 0
		tr.MaxIdleConns = 10
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}
			s3URL, errURL := url.Parse(ts.URL)
			if errURL != nil {
				log.Printf("url=%s parse error: %v", ts.URL, errURL)
				return nil, errURL
			}
			newAddr := s3URL.Host
			return dialer.DialContext(ctx, network, newAddr)
		}
	})

	cfg, errCfg := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("KEY", "SECRET", "SESSION")),
		config.WithHTTPClient(httpClient),
	)

	if errCfg != nil {
		log.Fatalf("LoadDefaultConfig: %v", errCfg)
	}

	// Create an Amazon S3 v2 client, important to use o.UsePathStyle
	// alternatively change local DNS settings, e.g., in /etc/hosts
	// to support requests to http://<bucketname>.127.0.0.1:32947/...
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(ts.URL)
	})

	//
	// now create the bucket
	//

	bucket := "newbucket"

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}

	if _, err := client.CreateBucket(context.TODO(), input); err != nil {
		t.Errorf("create bucket error: bucket=%s: %v", bucket, err)
	}
}

// go test -v -count 1 -run TestCreateBucketV2 ./test
func TestCreateBucketV2(t *testing.T) {
	client, stop := newFakeClient()
	defer stop()

	bucket := "newbucket"

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}

	if _, err := client.CreateBucket(context.TODO(), input); err != nil {
		t.Errorf("create bucket error: bucket=%s: %v", bucket, err)
	}
}

// go test -v -count 1 -run TestPutObjectV2 ./test
func TestPutObjectV2(t *testing.T) {
	client, stop := newFakeClient()
	defer stop()

	bucket := "newbucket"

	{
		input := &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		}
		if _, err := client.CreateBucket(context.TODO(), input); err != nil {
			t.Errorf("create bucket error: bucket=%s: %v", bucket, err)
		}
	}

	key := "test.txt"
	data := "test"

	{
		input := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(data)),
		}
		if _, err := client.PutObject(context.TODO(), input); err != nil {
			t.Errorf("put object error: bucket=%s: key=%s: %v",
				bucket, key, err)
		}
	}
}

// go test -v -count 1 -run TestGetObjectV2 ./test
func TestGetObjectV2(t *testing.T) {
	client, stop := newFakeClient()
	defer stop()

	bucket := "newbucket"

	{
		input := &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		}
		if _, err := client.CreateBucket(context.TODO(), input); err != nil {
			t.Errorf("create bucket error: bucket=%s: %v", bucket, err)
		}
	}

	key := "test.txt"
	data := "test"

	{
		input := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(data)),
		}
		if _, err := client.PutObject(context.TODO(), input); err != nil {
			t.Errorf("put object error: bucket=%s: key=%s: %v",
				bucket, key, err)
		}
	}

	{
		input := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		resp, err := client.GetObject(context.TODO(), input)
		if err != nil {
			t.Errorf("get object error: bucket=%s: key=%s: %v",
				bucket, key, err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("get object error: bucket=%s: key=%s: %v",
				bucket, key, err)
		}
		str := string(body)
		if str != data {
			t.Errorf("get object data error: bucket=%s: key=%s: expected=%s got=%s",
				bucket, key, data, str)
		}
	}
}

func newFakeClient() (*s3.Client, func()) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())

	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.Proxy = http.ProxyFromEnvironment
		tr.TLSClientConfig.InsecureSkipVerify = true
		tr.ExpectContinueTimeout = 0
		tr.MaxIdleConns = 10
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}
			s3URL, errURL := url.Parse(ts.URL)
			if errURL != nil {
				log.Printf("url=%s parse error: %v", ts.URL, errURL)
				return nil, errURL
			}
			newAddr := s3URL.Host
			return dialer.DialContext(ctx, network, newAddr)
		}
	})

	cfg, errCfg := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("KEY", "SECRET", "SESSION")),
		config.WithHTTPClient(httpClient),
		//config.WithRegion(region),
		//config.WithClientLogMode(aws.LogRetries|aws.LogRequestWithBody|aws.LogResponseWithBody),
	)

	if errCfg != nil {
		log.Fatalf("newFakeClient: %v", errCfg)
	}

	// Create an Amazon S3 v2 client, important to use o.UsePathStyle
	// alternatively change local DNS settings, e.g., in /etc/hosts
	// to support requests to http://<bucketname>.127.0.0.1:32947/...
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(ts.URL)
	})

	return client, func() { ts.Close() }
}
