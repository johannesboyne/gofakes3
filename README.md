![Logo](/GoFakeS3.png)
# AWS (GOFAKE)S3 

A golang AWS S3 fake server.

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

We're using it for running AWS lambda functions locally and to test browser based direct uploads to S3 locally.

## How to use it?

Do not use it at the moment ;-) I'll have to tune and refactor it a bit before.
But, you'll have to do the following:

- change `/etc/hosts`: `127.0.0.1 <bucket-name>.localhost`
- start the gofakes3 service

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

