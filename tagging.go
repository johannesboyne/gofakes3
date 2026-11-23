package gofakes3

import (
	"bytes"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"
)

// taggingMetaKey is the metadata key that holds the tags of an object. It is
// the header PutObject takes them from, and the value keeps its URL-encoded
// form ("k1=v1&k2=v2").
const taggingMetaKey = "X-Amz-Tagging"

// Limits on object tags, as enforced by S3.
const (
	maxObjectTags  = 10
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

// routeBucketTagging handles '?tagging' on a bucket. Bucket tags are not
// stored: GET answers as S3 does for a bucket without tags, DELETE has nothing
// to remove and PUT is refused rather than dropped.
func (g *GoFakeS3) routeBucketTagging(bucket string, w http.ResponseWriter, r *http.Request) error {
	if err := g.ensureBucketExists(bucket); err != nil {
		return err
	}

	switch r.Method {
	case "GET":
		return ResourceError(ErrNoSuchTagSet, bucket)
	case "PUT":
		return ErrNotImplemented
	case "DELETE":
		w.WriteHeader(http.StatusNoContent)
		return nil
	default:
		return ErrMethodNotAllowed
	}
}

func (g *GoFakeS3) getObjectTagging(bucket, object string, versionID VersionID, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "GET OBJECT TAGGING", bucket, object, versionID)

	if err := g.ensureBucketExists(bucket); err != nil {
		return err
	}

	var obj *Object
	var err error
	if versionID == "" {
		obj, err = g.storage.GetObject(bucket, object, nil)
	} else if g.versioned == nil {
		return ErrNotImplemented
	} else {
		obj, err = g.versioned.HeadObjectVersion(bucket, object, versionID)
	}
	if err != nil {
		return err
	}
	if obj == nil {
		return ErrInternal
	}
	obj.Contents.Close()
	if obj.IsDeleteMarker {
		return KeyNotFound(object)
	}

	// Tags stored before they were validated may not parse; return the rest.
	tags, _ := decodeTags(obj.Metadata[taggingMetaKey])

	if obj.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(obj.VersionID))
	}
	return g.xmlEncoder(w).Encode(Tagging{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		TagSet: TagSet{Tags: tags},
	})
}

func (g *GoFakeS3) putObjectTagging(bucket, object string, versionID VersionID, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "PUT OBJECT TAGGING", bucket, object, versionID)

	if err := g.ensureBucketExists(bucket); err != nil {
		return err
	}

	var tagging Tagging
	if err := g.xmlDecodeBody(r.Body, &tagging); err != nil {
		return err
	}
	if err := validateTags(tagging.TagSet.Tags); err != nil {
		return err
	}

	return g.setObjectTags(bucket, object, versionID, encodeTags(tagging.TagSet.Tags), http.StatusOK, w)
}

func (g *GoFakeS3) deleteObjectTagging(bucket, object string, versionID VersionID, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "DELETE OBJECT TAGGING", bucket, object, versionID)

	if err := g.ensureBucketExists(bucket); err != nil {
		return err
	}

	return g.setObjectTags(bucket, object, versionID, "", http.StatusNoContent, w)
}

// tagWriteAttempts is how often setObjectTags reads and writes an object
// again when it was replaced in the meantime.
const tagWriteAttempts = 5

// setObjectTags replaces the tags of the current version of an object with
// tags, as encoded by encodeTags, and writes the response.
func (g *GoFakeS3) setObjectTags(bucket, object string, versionID VersionID, tags string, status int, w http.ResponseWriter) error {
	for attempt := 1; ; attempt++ {
		version, err := g.writeObjectTags(bucket, object, versionID, tags)
		if HasErrorCode(err, ErrPreconditionFailed) {
			if attempt < tagWriteAttempts {
				continue
			}
			return ErrConditionalRequestConflict
		} else if err != nil {
			return err
		}

		if version != "" {
			w.Header().Set("x-amz-version-id", string(version))
		}
		w.WriteHeader(status)
		return nil
	}
}

// writeObjectTags stores tags with the current version of an object. The
// Backend interface cannot change the metadata of an object on its own, so the
// object is written again with its contents and the new metadata; in a
// versioned bucket, this creates a new version. The write is conditional on the
// ETag that was read and fails with ErrPreconditionFailed if the object was
// replaced in the meantime.
func (g *GoFakeS3) writeObjectTags(bucket, object string, versionID VersionID, tags string) (VersionID, error) {
	obj, err := g.storage.GetObject(bucket, object, nil)
	if err != nil {
		return "", err
	}
	if obj == nil {
		return "", ErrInternal
	}

	if versionID != "" && versionID != obj.VersionID {
		// Writing an older version again would make it the current one.
		obj.Contents.Close()
		return "", ErrNotImplemented
	}

	if obj.Metadata[taggingMetaKey] == tags {
		obj.Contents.Close()
		return obj.VersionID, nil
	}

	// Read the whole body first: a backend may truncate the stored object
	// before it reads the new contents.
	body, err := ReadAll(obj.Contents, obj.Size)
	obj.Contents.Close()
	if err != nil {
		return "", err
	}

	meta := make(map[string]string, len(obj.Metadata)+1)
	for k, v := range obj.Metadata {
		meta[k] = v
	}
	// An empty value rather than no key, so that a backend which still merges
	// the previous metadata into a PUT does not bring the old tags back.
	meta[taggingMetaKey] = tags

	etag := FormatETag(obj.Hash)
	result, err := g.storage.PutObject(bucket, object, meta, bytes.NewReader(body), int64(len(body)), &PutConditions{IfMatch: &etag})
	if err != nil {
		return "", err
	}
	return result.VersionID, nil
}

// validateTaggingHeader checks the tags sent with PutObject, CopyObject or
// CreateMultipartUpload in the x-amz-tagging header, if there is one.
func validateTaggingHeader(meta map[string]string) error {
	value, ok := meta[taggingMetaKey]
	if !ok {
		return nil
	}
	tags, err := decodeTags(value)
	if err != nil {
		return err
	}
	return validateTags(tags)
}

// validateTags checks a tag set against the S3 limits.
func validateTags(tags []Tag) error {
	if len(tags) > maxObjectTags {
		return ErrorMessage(ErrInvalidTag, "Object tags cannot be greater than 10")
	}

	seen := make(map[string]bool, len(tags))
	for _, tag := range tags {
		switch {
		case tag.Key == "" || utf8.RuneCountInString(tag.Key) > maxTagKeyLen:
			return ErrorMessagef(ErrInvalidTag, "The TagKey you have provided is invalid: %q", tag.Key)
		case utf8.RuneCountInString(tag.Value) > maxTagValueLen:
			return ErrorMessagef(ErrInvalidTag, "The TagValue you have provided is too long for key %q", tag.Key)
		case strings.HasPrefix(strings.ToLower(tag.Key), "aws:"):
			return ErrorMessage(ErrInvalidTag, "Your TagKey cannot be prefixed with aws:")
		case seen[tag.Key]:
			return ErrorMessagef(ErrInvalidTag, "Cannot provide multiple Tags with the same key %q", tag.Key)
		}
		seen[tag.Key] = true
	}
	return nil
}

// encodeTags returns tags in the URL-encoded form of the x-amz-tagging header.
func encodeTags(tags []Tag) string {
	values := make(url.Values, len(tags))
	for _, tag := range tags {
		values.Add(tag.Key, tag.Value)
	}
	return values.Encode()
}

// decodeTags parses tags in the form of the x-amz-tagging header, sorted by
// key. On a malformed value, it returns the tags that did parse and an
// InvalidTag error.
func decodeTags(value string) ([]Tag, error) {
	values, err := url.ParseQuery(value)
	if err != nil {
		err = ErrorMessage(ErrInvalidTag, "The x-amz-tagging header is not URL-encoded")
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var tags []Tag
	for _, key := range keys {
		for _, v := range values[key] {
			tags = append(tags, Tag{Key: key, Value: v})
		}
	}
	return tags, err
}
