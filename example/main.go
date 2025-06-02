package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func main() {
	// Step 1: Create a new gofakes3 server with in-memory backend
	backend := s3mem.New()
	logger := log.New(os.Stderr, "[GoFakeS3] ", log.LstdFlags)
	faker := gofakes3.New(backend, 
		gofakes3.WithLogger(gofakes3.StdLog(logger)),
	)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	fmt.Printf("GoFakeS3 server running at: %s\n", ts.URL)

	// Step a: Configure the AWS SDK v2 S3 client
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", "")),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: ts.URL}, nil
			}),
		),
	)
	if err != nil {
		log.Fatalf("Unable to configure AWS SDK: %v", err)
	}

	// Create S3 client with path-style addressing
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Step 3: Create a bucket
	bucketName := "test-bucket"
	_, err = client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}
	fmt.Printf("Created bucket: %s\n", bucketName)

	// Step 4: Upload an object to the bucket
	objectKey := "hello.txt"
	objectContent := "Hello, GoFakeS3!"
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte(objectContent)),
	})
	if err != nil {
		log.Fatalf("Failed to upload object: %v", err)
	}
	fmt.Printf("Uploaded object: %s/%s\n", bucketName, objectKey)

	// Step 5: Download the object
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Fatalf("Failed to download object: %v", err)
	}
	defer getResp.Body.Close()

	downloadedContent, err := io.ReadAll(getResp.Body)
	if err != nil {
		log.Fatalf("Failed to read object content: %v", err)
	}
	fmt.Printf("Downloaded object content: %s\n", downloadedContent)

	// Step 6: List objects in the bucket
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Fatalf("Failed to list objects: %v", err)
	}
	fmt.Printf("Objects in bucket %s:\n", bucketName)
	for _, obj := range listResp.Contents {
		fmt.Printf("- %s (size: %d bytes, last modified: %s)\n", 
			*obj.Key, obj.Size, obj.LastModified)
	}

	// Step 7: Delete the object
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Fatalf("Failed to delete object: %v", err)
	}
	fmt.Printf("Deleted object: %s/%s\n", bucketName, objectKey)
	
	fmt.Println("Example completed successfully!")
}