# GoFakeS3 Examples with AWS SDK v2

This directory contains examples that demonstrate how to use GoFakeS3 with AWS SDK v2.

## Examples Included

1. **Integrated Example** (`main.go`) - Creates an in-memory S3 server and a client in the same process
2. **Standalone Server** (`server.go`) - Runs a GoFakeS3 server on a specific port
3. **Client Example** (`client.go`) - Connects to a running GoFakeS3 server

## Example 1: Integrated Server and Client

This example creates an in-memory S3 server and client in the same process:

```sh
go run main.go
```

### What This Demonstrates

1. Setting up a temporary in-memory S3 server
2. Configuring AWS SDK v2 to connect to the server
3. Basic S3 operations: create bucket, upload, download, list, and delete objects

### Expected Output

```
GoFakeS3 server running at: http://127.0.0.1:xxxxx
Created bucket: test-bucket
Uploaded object: test-bucket/hello.txt
Downloaded object content: Hello, GoFakeS3!
Objects in bucket test-bucket:
- hello.txt (size: 15 bytes, last modified: 2023-xx-xx xx:xx:xx)
Deleted object: test-bucket/hello.txt
Example completed successfully!
```

## Example 2: Standalone Server

This example runs a standalone GoFakeS3 server that you can connect to with various clients:

```sh
# Run server on default port (9000)
go run server.go

# Or specify a custom port
go run server.go :8080
```

### Features Demonstrated

1. Running GoFakeS3 as a standalone service
2. Auto-creating buckets (optional feature)
3. Graceful shutdown handling
4. Creating a default "example-bucket" on startup

## Example 3: Client Example

This example connects to a running GoFakeS3 server:

```sh
# First start the server in another terminal
go run server.go

# Then run the client (connects to localhost:9000 by default)
go run client.go

# Or connect to a custom URL
go run client.go http://localhost:8080
```

### What This Demonstrates

1. Connecting to an external GoFakeS3 server
2. Configuring AWS SDK v2 clients
3. Basic S3 operations: upload, download, and list objects

## Using with Your Own Applications

To use GoFakeS3 with your own applications:

1. Start the standalone server:
   ```
   go run server.go
   ```

2. Configure your AWS SDK clients to use the server URL (e.g., http://localhost:9000)
   with path-style addressing enabled.

3. Use the same credentials provided in the client examples, or configure GoFakeS3
   to use your own credentials.