package s3mem

import "time"

type bucketItem struct {
	key          string
	lastModified time.Time
	data         []byte
	hash         []byte
	metadata     map[string]string
}

type bucket struct {
	name string
	data map[string]*bucketItem
}
