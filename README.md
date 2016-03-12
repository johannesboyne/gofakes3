![Logo](/GoFakeS3.png)
# AWS (GOFAKE)S3 

[![Build Status](https://drone.io/github.com/johannesboyne/gofakes3/status.png)](https://drone.io/github.com/johannesboyne/gofakes3/latest)

AWS S3 fake server.

A _poor man's_ object storage based on [BoltDB](https://github.com/boltdb/bolt).

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

Please feel free to check it out and to provide useful feedback (using github issues), but be aware, this software is used internally and for the local development only. Thus, it has no demand for correctness, performance or security.

**For setting it up locally, you'll have to do the following:**

- add the following to your `/etc/hosts`: `127.0.0.1 <bucket-name>.localhost`
- start the gofakes3 service, e.g.: `./s3f_darwin_amd64 -db tests3.db -port ":9000"`

### Exemplary usage

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
## Missing

- [] Delete Buckets and Objects

## Similar notable projects

- https://github.com/andrewgaul/s3proxy by @andrewgaul
