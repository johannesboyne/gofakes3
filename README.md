[![Codecov](https://codecov.io/gh/johannesboyne/gofakes3/branch/master/graph/badge.svg)](https://codecov.io/gh/johannesboyne/gofakes3)

![Logo](/GoFakeS3.png)

# AWS (GOFAKE)S3

AWS S3 fake server and testing library for comprehensive S3 integration testing.
This tool can be used to run a test server, for example, to support testing AWS Lambda functions that interact with S3. It also serves as a straightforward and convenient S3 mock and test server for various development needs.

## Intended Use

**GOFAKE)S3** is primarily designed for:

- Local development of S3-dependent AWS Lambda functions.
- Testing implementations with AWS S3 access.
- Facilitating browser-based direct uploads to S3 in a local testing environment.

## When Not to Use (GOFAKE)S3?

**(GOFAKE)S3** should not be used as a production service. Its primary purpose is to aid in development and testing:

- **(GOFAKE)S3** is not designed for production-level data storage or handling.
- It lacks the robustness required for safe, persistent access to production data.
- The tool is still under development with significant portions of the AWS S3 API yet to be implemented. Consequently, breaking changes are expected.

For production environments, consider more established solutions. Some recommended alternatives can be found in the "Similar Notable Projects" section below.

## How to use it?

### Example with AWS SDK v2 (Recommended)

```golang
import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// Set up gofakes3 server
backend := s3mem.New()
faker := gofakes3.New(backend)
ts := httptest.NewServer(faker.Server())
defer ts.Close()

// Setup AWS SDK v2 config
cfg, err := config.LoadDefaultConfig(
	context.TODO(),
	config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("ACCESS_KEY", "SECRET_KEY", "")),
	config.WithHTTPClient(&http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}),
	config.WithEndpointResolverWithOptions(
		aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: ts.URL}, nil
		}),
	),
)
if err != nil {
	panic(err)
}

// Create an Amazon S3 v2 client, important to use o.UsePathStyle
client := s3.NewFromConfig(cfg, func(o *s3.Options) {
	o.UsePathStyle = true
})

// Create a new bucket
_, err = client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
	Bucket: aws.String("newbucket"),
})
if err != nil {
	panic(err)
}

// Upload an object
_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
	Body:   strings.NewReader(`{"configuration": {"main_color": "#333"}, "screens": []}`),
	Bucket: aws.String("newbucket"),
	Key:    aws.String("test.txt"),
})
if err != nil {
	panic(err)
}

// ... accessing of test.txt through any S3 client would now be possible
```

Please feel free to check it out and to provide useful feedback (using github
issues), but be aware, this software is used internally and for the local
development only. Thus, it has no demand for correctness, performance or
security.

## Connection Options

There are different ways to connect to your local GoFakeS3 server:

### 1. Path-Style Addressing (Recommended)

Path-style is the most flexible and least restrictive approach, where the bucket name appears in the URL path:

```
http://localhost:9000/mybucket/myobject
```

With AWS SDK v2, configure this using:

```golang
client := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.UsePathStyle = true
})
```

### 2. Virtual-Hosted Style Addressing

In this mode, the bucket name is part of the hostname:

```
http://mybucket.localhost:9000/myobject
```

This requires DNS configuration. If using `localhost` as your endpoint, add the following to `/etc/hosts` for _every bucket you want to use_:

```
127.0.0.1 mybucket.localhost
```

With AWS SDK v2, this is the default mode when not setting `UsePathStyle`:

```golang
client := s3.NewFromConfig(cfg)
```

### 3. Environment Variable (AWS SDK v2)

With AWS SDK v2, you can also set an environment variable to specify the endpoint:

```
os.Setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
```

This approach works with code that doesn't directly configure the S3 client.

## Exemplary usage

### Lambda Example with AWS SDK v3 for JavaScript

```javascript
// Using AWS SDK v3 for JavaScript
import { S3Client, CreateBucketCommand } from "@aws-sdk/client-s3";

// Create an S3 client with custom endpoint
const s3Client = new S3Client({
  region: "us-east-1",
  endpoint: "http://localhost:9000",
  forcePathStyle: true, // Required for GoFakeS3
  credentials: {
    accessKeyId: "ACCESS_KEY",
    secretAccessKey: "SECRET_KEY",
  },
});

// Lambda handler using async/await
export const handler = async (event, context) => {
  try {
    const command = new CreateBucketCommand({
      Bucket: "my-bucket",
    });

    const response = await s3Client.send(command);
    return response;
  } catch (error) {
    console.error("Error:", error);
    throw error;
  }
};
```

### Legacy Lambda Example (AWS SDK v2 for JavaScript)

```javascript
var AWS = require("aws-sdk");

var ep = new AWS.Endpoint("http://localhost:9000");
var s3 = new AWS.S3({
  endpoint: ep,
  s3ForcePathStyle: true, // Recommended for GoFakeS3
});

exports.handle = function (e, ctx) {
  s3.createBucket(
    {
      Bucket: "my-bucket",
    },
    function (err, data) {
      if (err) return console.log(err, err.stack);
      ctx.succeed(data);
    },
  );
};
```

### Upload Example

```html
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
  </head>
  <body>

  <form action="http://localhost:9000/<bucket-name>/" method="post" enctype="multipart/form-data">
    Key to upload:
    <input type="input"  name="key" value="user/user1/test/<filename>" /><br />
    <input type="hidden" name="acl" value="public-read" />
    <input type="hidden" name="x-amz-meta-uuid" value="14365123651274" />
    <input type="hidden" name="x-amz-server-side-encryption" value="AES256" />
    <input type="text"   name="X-Amz-Credential" value="AKIAIOSFODNN7EXAMPLE/20151229/us-east-1/s3/aws4_request" />
    <input type="text"   name="X-Amz-Algorithm" value="AWS4-HMAC-SHA256" />
    <input type="text"   name="X-Amz-Date" value="20151229T000000Z" />

    Tags for File:
    <input type="input"  name="x-amz-meta-tag" value="" /><br />
    <input type="hidden" name="Policy" value='<Base64-encoded policy string>' />
    <input type="hidden" name="X-Amz-Signature" value="<signature-value>" />
    File:
    <input type="file"   name="file" /> <br />
    <!-- The elements after this will be ignored -->
    <input type="submit" name="submit" value="Upload to Amazon S3" />
  </form>
</html>
```

### Example with AWS SDK v1 (Legacy)

```golang
// fake s3
backend := s3mem.New()
faker := gofakes3.New(backend)
ts := httptest.NewServer(faker.Server())
defer ts.Close()

// configure S3 client
s3Config := &aws.Config{
	Credentials:      credentials.NewStaticCredentials("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ""),
	Endpoint:         aws.String(ts.URL),
	Region:           aws.String("eu-central-1"),
	DisableSSL:       aws.Bool(true),
	S3ForcePathStyle: aws.Bool(true),
}
newSession := session.New(s3Config)

s3Client := s3.New(newSession)
cparams := &s3.CreateBucketInput{
	Bucket: aws.String("newbucket"),
}

// Create a new bucket using the CreateBucket call.
_, err := s3Client.CreateBucket(cparams)
if err != nil {
	// Message from an error.
	fmt.Println(err.Error())
	return
}

// Upload a new object "testobject" with the string "Hello World!" to our "newbucket".
_, err = s3Client.PutObject(&s3.PutObjectInput{
	Body:   strings.NewReader(`{"configuration": {"main_color": "#333"}, "screens": []}`),
	Bucket: aws.String("newbucket"),
	Key:    aws.String("test.txt"),
})

// ... accessing of test.txt through any S3 client would now be possible
```

## Similar notable projects

- https://github.com/minio/minio **not similar but powerfull ;-)**
- https://github.com/andrewgaul/s3proxy by @andrewgaul

## Contributors

A big thank you to all the [contributors](https://github.com/johannesboyne/gofakes3/graphs/contributors),
especially [Blake @shabbyrobe](https://github.com/shabbyrobe) who pushed this
little project to the next level!

**Help wanted**
