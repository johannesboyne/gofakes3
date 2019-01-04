[![CircleCI](https://circleci.com/gh/johannesboyne/gofakes3.svg?style=svg)](https://circleci.com/gh/johannesboyne/gofakes3)
[![Codecov](https://codecov.io/gh/johannesboyne/gofakes3/branch/master/graph/badge.svg)](https://codecov.io/gh/johannesboyne/gofakes3)

![Logo](/GoFakeS3.png)
# AWS (GOFAKE)S3 

AWS S3 fake server.

A _poor man's_ object storage based on an In-Memory KV db or [BoltDB](https://github.com/boltdb/bolt) (Pluggable).

```
  s3client -> [gofakes3:9000] -- Get    Bucket (List)
                          ^  |-- Create Bucket
                          |  |-- Delete Bucket
                          |  |-- Head   Bucket
                          |  |
                          |  |-- Get    Object
                          |  |-- Create Object
                          |  |-- Delete Object
                          |  |-- Head   Object
                          |  V
                   XXXXXXXXXXXXXXXXXXXXX
                   XXXX             XXXX
                XXXX                   XXXX
                XX XXX                XXXXX
                XX   XXXXXXXXXXXXXXXXXX  XX
                XX                       XX
                XX                       XX
                XX     BoltDB (Store)    XX
                XX           ⚡️           XX
                XX                       XX
                XX                      XXX
                 XXX                 XXXX
                   XXXXXX         XXXX
                         XXXXXXXXX
```

## What to use it for?

We're using it for the local development of S3 dependent Lambda functions and to test browser based direct uploads to S3 locally.

## How to use it?

Please feel free to check it out and to provide useful feedback (using github
issues), but be aware, this software is used internally and for the local
development only. Thus, it has no demand for correctness, performance or
security.

There are two ways to run locally: using DNS, or using S3 path mode.

S3 path mode is the most flexible and least restrictive, but it does require that you
are able to modify your client code.In Go, the modification would look like so:
    
	config := aws.Config{}
	config.WithS3ForcePathStyle(true)

S3 path mode works over the network by default for all bucket names.

If you are unable to modify the code, DNS mode can be used, but it comes with further
restrictions and requires you to be able to modify your local DNS resolution.

If using `localhost` as your endpoint, you will need to add the following to
`/etc/hosts` for *every bucket you want to fake*:

    127.0.0.1 <bucket-name>.localhost

It is trickier if you want other machines to be able to use your fake S3 server
as you need to be able to modify their DNS resolution as well.


## Exemplary usage

### Lambda Example

```javascript
var AWS   = require('aws-sdk')

var ep = new AWS.Endpoint('http://localhost:9000');
var s3 = new AWS.S3({endpoint: ep});

exports.handle = function (e, ctx) {
  s3.createBucket({
    Bucket: '<bucket-name>',
  }, function(err, data) {
    if (err) return console.log(err, err.stack);
    ctx.succeed(data)
  });
}
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

## Similar notable projects

- https://github.com/minio/minio **not similar but powerfull ;-)**
- https://github.com/andrewgaul/s3proxy by @andrewgaul

## Contributors

- [Johannes Boyne @johannesboyne](https://github.com/johannesboyne)
- [Blake Williams @shabbyrobe](https://github.com/shabbyrobe)
