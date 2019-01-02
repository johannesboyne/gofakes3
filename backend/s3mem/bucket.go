package s3mem

import (
	"time"

	"github.com/johannesboyne/gofakes3"
)

type bucketItem struct {
	key          string
	lastModified time.Time
	data         []byte
	hash         []byte
	metadata     map[string]string
}

type bucket struct {
	name         string
	creationDate gofakes3.ContentTime
	data         map[string]*bucketItem
}
