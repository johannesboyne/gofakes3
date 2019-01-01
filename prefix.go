package gofakes3

import (
	"fmt"
	"net/url"
	"strings"
)

type Prefix struct {
	HasPrefix bool
	Prefix    string

	HasDelimiter bool
	Delimiter    string
}

func prefixFromQuery(query url.Values) Prefix {
	prefix := Prefix{
		Prefix:    query.Get("prefix"),
		Delimiter: query.Get("delimiter"),
	}
	_, prefix.HasPrefix = query["prefix"]
	_, prefix.HasDelimiter = query["delimiter"]
	return prefix
}

// PrefixMatch checks whether key starts with prefix. If the prefix does not
// match, nil is returned.
//
// It is a best-effort attempt to implement the prefix/delimiter matching found
// in S3.
//
// To check whether the key belongs in Contents or CommonPrefixes, compare the
// result to key.
//
func (p Prefix) Match(key string) (match *PrefixMatch) {
	if !p.HasPrefix {
		// If there is no prefix, in the search, the match is the prefix:
		return &PrefixMatch{Key: key, MatchedPart: key}
	}

	if !p.HasDelimiter {
		// If the request does not contain a delimiter, prefix matching is a
		// simple string prefix:
		if strings.HasPrefix(key, p.Prefix) {
			return &PrefixMatch{Key: key, MatchedPart: p.Prefix}
		}
		return nil
	}

	// Delimited + Prefix matches, for example:
	//	 $ aws s3 ls s3://my-bucket/
	//	                            PRE AWSLogs/
	//	 $ aws s3 ls s3://my-bucket/AWSLogs
	//	                            PRE AWSLogs/
	//	 $ aws s3 ls s3://my-bucket/AWSLogs/
	//	                            PRE 260839334643/
	//	 $ aws s3 ls s3://my-bucket/AWSLogs/2608
	//	                            PRE 260839334643/

	keyParts := strings.Split(strings.TrimLeft(key, p.Delimiter), p.Delimiter)
	preParts := strings.Split(strings.TrimLeft(p.Prefix, p.Delimiter), p.Delimiter)

	if len(keyParts) < len(preParts) {
		return nil
	}

	// If the key exactly matches the prefix, but only up to a delimiter,
	// AWS appends the delimiter to the result:
	//	 $ aws s3 ls s3://my-bucket/AWSLogs
	//	                            PRE AWSLogs/
	appendDelim := len(keyParts) != len(preParts)
	matched := 0

	last := len(preParts) - 1
	for i := 0; i < len(preParts); i++ {
		if i == last {
			if !strings.HasPrefix(keyParts[i], preParts[i]) {
				return nil
			}

		} else {
			if keyParts[i] != preParts[i] {
				return nil
			}
		}
		matched++
	}

	if matched == 0 {
		return nil
	}

	out := strings.Join(keyParts[:matched], p.Delimiter)
	if appendDelim {
		out += p.Delimiter
	}

	return &PrefixMatch{Key: key, CommonPrefix: out != key, MatchedPart: out}
}

func (p Prefix) String() string {
	if !p.HasPrefix {
		return "<prefix empty>"
	}
	if p.HasDelimiter {
		return fmt.Sprintf("prefix:%q, delim:%q", p.Prefix, p.Delimiter)
	} else {
		return fmt.Sprintf("prefix:%q", p.Prefix)
	}
}

type PrefixMatch struct {
	// Input key passed to PrefixMatch.
	Key string

	// CommonPrefix indicates whether this key should be returned in the bucket
	// contents or the common prefixes part of the "list bucket" response.
	CommonPrefix bool

	// The longest matched part of the key.
	MatchedPart string
}
