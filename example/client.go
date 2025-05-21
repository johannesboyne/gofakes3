package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	// Server URL - defaults to localhost:9000, but can be overridden
	serverURL := "http://localhost:9000"
	if len(os.Args) > 1 {
		serverURL = os.Args[1]
	}
	
	fmt.Printf("Connecting to GoFakeS3 server at: %s\n", serverURL)

	// Configure AWS SDK v2
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"YOUR-ACCESSKEYID", 
			"YOUR-SECRETACCESSKEY", 
			"",
		)),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: serverURL}, nil
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

	// Define the bucket and object names
	bucketName := "example-bucket" // This should match the bucket created in server.go
	objectKey := "hello.txt"
	objectContent := "Hello from the S3 client!"

	// Upload a file
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte(objectContent)),
	})
	if err != nil {
		log.Fatalf("Failed to upload object: %v\nMake sure the GoFakeS3 server is running at %s", err, serverURL)
	}
	fmt.Printf("Uploaded object: %s/%s\n", bucketName, objectKey)

	// Download the file
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	
	getResp, err := client.GetObject(ctx2, &s3.GetObjectInput{
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
	fmt.Printf("Downloaded object content: %s\n", string(downloadedContent))

	// List all objects in the bucket
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	
	listResp, err := client.ListObjectsV2(ctx3, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Fatalf("Failed to list objects: %v", err)
	}
	
	fmt.Printf("Objects in bucket %s:\n", bucketName)
	if len(listResp.Contents) == 0 {
		fmt.Println("  No objects found")
	} else {
		for _, obj := range listResp.Contents {
			fmt.Printf("  - %s (size: %d bytes)\n", *obj.Key, obj.Size)
		}
	}
	
	// Client operations completed successfully
	fmt.Println("Client operations completed successfully!")
}