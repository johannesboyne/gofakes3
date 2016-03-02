from cStringIO import StringIO
import boto.exception
import boto.s3.connection
import boto.s3.acl
import bunch
import datetime
import time
import email.utils
import isodate
import nose
import operator
import socket
import ssl
import os
import requests
import base64
import hmac
import sha
import pytz
import json
import httplib2
import threading
import itertools
import string
import random
import re

import xml.etree.ElementTree as ET

from httplib import HTTPConnection, HTTPSConnection
from urlparse import urlparse

from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from .utils import assert_raises
from .utils import generate_random
from .utils import region_sync_meta
import AnonymousAuth

from email.header import decode_header
from ordereddict import OrderedDict

from boto.s3.cors import CORSConfiguration

from . import (
    nuke_prefixed_buckets,
    get_new_bucket,
    get_new_bucket_name,
    s3,
    targets,
    config,
    get_prefix,
    is_slow_backend,
    )


NONEXISTENT_EMAIL = 'doesnotexist@dreamhost.com.invalid'

def not_eq(a, b):
    assert a != b, "%r == %r" % (a, b)

def check_access_denied(fn, *args, **kwargs):
    e = assert_raises(boto.exception.S3ResponseError, fn, *args, **kwargs)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


def check_grants(got, want):
    """
    Check that grants list in got matches the dictionaries in want,
    in any order.
    """
    eq(len(got), len(want))
    got = sorted(got, key=operator.attrgetter('id'))
    want = sorted(want, key=operator.itemgetter('id'))
    for g, w in zip(got, want):
        w = dict(w)
        eq(g.permission, w.pop('permission'))
        eq(g.id, w.pop('id'))
        eq(g.display_name, w.pop('display_name'))
        eq(g.uri, w.pop('uri'))
        eq(g.email_address, w.pop('email_address'))
        eq(g.type, w.pop('type'))
        eq(w, {})

def check_aws4_support():
    if 'S3_USE_SIGV4' not in os.environ:
        raise SkipTest

def tag(*tags):
    def wrap(func):
        for tag in tags:
            setattr(func, tag, True)
        return func
    return wrap

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty buckets return no contents')
def test_bucket_list_empty():
    bucket = get_new_bucket()
    l = bucket.list()
    l = list(l)
    eq(l, [])

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='distinct buckets have different contents')
def test_bucket_list_distinct():
    bucket1 = get_new_bucket()
    bucket2 = get_new_bucket()
    key = bucket1.new_key('asdf')
    key.set_contents_from_string('asdf')
    l = bucket2.list()
    l = list(l)
    eq(l, [])

def _create_keys(bucket=None, keys=[]):
    """
    Populate a (specified or new) bucket with objects with
    specified names (and contents identical to their names).
    """
    if bucket is None:
        bucket = get_new_bucket()

    for s in keys:
        key = bucket.new_key(s)
        key.set_contents_from_string(s)

    return bucket


def _get_keys_prefixes(li):
    """
    figure out which of the strings in a list are actually keys
    return lists of strings that are (keys) and are not (prefixes)
    """
    keys = [x for x in li if isinstance(x, boto.s3.key.Key)]
    prefixes = [x for x in li if not isinstance(x, boto.s3.key.Key)]
    return (keys, prefixes)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=2, no marker')
def test_bucket_list_many():
    bucket = _create_keys(keys=['foo', 'bar', 'baz'])

    # bucket.list() is high-level and will not let us set max-keys,
    # using it would require using >1000 keys to test, and that would
    # be too slow; use the lower-level call bucket.get_all_keys()
    # instead
    l = bucket.get_all_keys(max_keys=2)
    eq(len(l), 2)
    eq(l.is_truncated, True)
    names = [e.name for e in l]
    eq(names, ['bar', 'baz'])

    l = bucket.get_all_keys(max_keys=2, marker=names[-1])
    eq(len(l), 1)
    eq(l.is_truncated, False)
    names = [e.name for e in l]
    eq(names, ['foo'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefixes in multi-component object names')
def test_bucket_list_delimiter_basic():
    bucket = _create_keys(keys=['foo/bar', 'foo/baz/xyzzy', 'quux/thud', 'asdf'])

    # listings should treat / delimiter in a directory-like fashion
    li = bucket.list(delimiter='/')
    eq(li.delimiter, '/')

    # asdf is the only terminal object that should appear in the listing
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['asdf'])

    # In Amazon, you will have two CommonPrefixes elements, each with a single
    # prefix. According to Amazon documentation
    # (http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGET.html),
    # the response's CommonPrefixes should contain all the prefixes, which DHO
    # does.
    #
    # Unfortunately, boto considers a CommonPrefixes element as a prefix, and
    # will store the last Prefix element within a CommonPrefixes element,
    # effectively overwriting any other prefixes.

    # the other returned values should be the pure prefixes foo/ and quux/
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['foo/', 'quux/'])

def validate_bucket_list(bucket, prefix, delimiter, marker, max_keys,
                         is_truncated, check_objs, check_prefixes, next_marker):
    #
    li = bucket.get_all_keys(delimiter=delimiter, prefix=prefix, max_keys=max_keys, marker=marker)

    eq(li.is_truncated, is_truncated)
    eq(li.next_marker, next_marker)

    (keys, prefixes) = _get_keys_prefixes(li)

    eq(len(keys), len(check_objs))
    eq(len(prefixes), len(check_prefixes))

    objs = [e.name for e in keys]
    eq(objs, check_objs)

    prefix_names = [e.name for e in prefixes]
    eq(prefix_names, check_prefixes)

    return li.next_marker

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefixes in multi-component object names')
def test_bucket_list_delimiter_prefix():
    bucket = _create_keys(keys=['asdf', 'boo/bar', 'boo/baz/xyzzy', 'cquux/thud', 'cquux/bla'])

    delim = '/'
    marker = ''
    prefix = ''

    marker = validate_bucket_list(bucket, prefix, delim, '', 1, True, ['asdf'], [], 'asdf')
    marker = validate_bucket_list(bucket, prefix, delim, marker, 1, True, [], ['boo/'], 'boo/')
    marker = validate_bucket_list(bucket, prefix, delim, marker, 1, False, [], ['cquux/'], None)

    marker = validate_bucket_list(bucket, prefix, delim, '', 2, True, ['asdf'], ['boo/'], 'boo/')
    marker = validate_bucket_list(bucket, prefix, delim, marker, 2, False, [], ['cquux/'], None)

    prefix = 'boo/'

    marker = validate_bucket_list(bucket, prefix, delim, '', 1, True, ['boo/bar'], [], 'boo/bar')
    marker = validate_bucket_list(bucket, prefix, delim, marker, 1, False, [], ['boo/baz/'], None)

    marker = validate_bucket_list(bucket, prefix, delim, '', 2, False, ['boo/bar'], ['boo/baz/'], None)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='prefix and delimiter handling when object ends with delimiter')
def test_bucket_list_delimiter_prefix_ends_with_delimiter():
    bucket = _create_keys(keys=['asdf/'])
    validate_bucket_list(bucket, 'asdf/', '/', '', 1000, False, ['asdf/'], [], None)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-slash delimiter characters')
def test_bucket_list_delimiter_alt():
    bucket = _create_keys(keys=['bar', 'baz', 'cab', 'foo'])

    li = bucket.list(delimiter='a')
    eq(li.delimiter, 'a')

    # foo contains no 'a' and so is a complete key
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['ba', 'ca'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='percentage delimiter characters')
def test_bucket_list_delimiter_percentage():
    bucket = _create_keys(keys=['b%ar', 'b%az', 'c%ab', 'foo'])

    li = bucket.list(delimiter='%')
    eq(li.delimiter, '%')

    # foo contains no 'a' and so is a complete key
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['b%', 'c%'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='whitespace delimiter characters')
def test_bucket_list_delimiter_whitespace():
    bucket = _create_keys(keys=['b ar', 'b az', 'c ab', 'foo'])

    li = bucket.list(delimiter=' ')
    eq(li.delimiter, ' ')

    # foo contains no 'a' and so is a complete key
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['b ', 'c '])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='dot delimiter characters')
def test_bucket_list_delimiter_dot():
    bucket = _create_keys(keys=['b.ar', 'b.az', 'c.ab', 'foo'])

    li = bucket.list(delimiter='.')
    eq(li.delimiter, '.')

    # foo contains no 'a' and so is a complete key
    (keys,prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo'])

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefix_names = [e.name for e in prefixes]
    eq(len(prefixes), 2)
    eq(prefix_names, ['b.', 'c.'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='non-printable delimiter can be specified')
def test_bucket_list_delimiter_unreadable():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket = _create_keys(keys=key_names)

    li = bucket.list(delimiter='\x0a')
    eq(li.delimiter, '\x0a')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='empty delimiter can be specified')
def test_bucket_list_delimiter_empty():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket = _create_keys(keys=key_names)

    li = bucket.list(delimiter='')
    eq(li.delimiter, '')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])



@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='unspecified delimiter defaults to none')
def test_bucket_list_delimiter_none():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket = _create_keys(keys=key_names)

    li = bucket.list()
    eq(li.delimiter, '')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list')
@attr(assertion='unused delimiter is not found')
def test_bucket_list_delimiter_not_exist():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket = _create_keys(keys=key_names)

    li = bucket.list(delimiter='/')
    eq(li.delimiter, '/')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='returns only objects under prefix')
def test_bucket_list_prefix_basic():
    bucket = _create_keys(keys=['foo/bar', 'foo/baz', 'quux'])

    li = bucket.list(prefix='foo/')
    eq(li.prefix, 'foo/')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo/bar', 'foo/baz'])
    eq(prefixes, [])


# just testing that we can do the delimeter and prefix logic on non-slashes
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='prefixes w/o delimiters')
def test_bucket_list_prefix_alt():
    bucket = _create_keys(keys=['bar', 'baz', 'foo'])

    li = bucket.list(prefix='ba')
    eq(li.prefix, 'ba')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['bar', 'baz'])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='empty prefix returns everything')
def test_bucket_list_prefix_empty():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket = _create_keys(keys=key_names)

    li = bucket.list(prefix='')
    eq(li.prefix, '')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='unspecified prefix returns everything')
def test_bucket_list_prefix_none():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket = _create_keys(keys=key_names)

    li = bucket.list()
    eq(li.prefix, '')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, key_names)
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='nonexistent prefix returns nothing')
def test_bucket_list_prefix_not_exist():
    bucket = _create_keys(keys=['foo/bar', 'foo/baz', 'quux'])

    li = bucket.list(prefix='d')
    eq(li.prefix, 'd')

    (keys, prefixes) = _get_keys_prefixes(li)
    eq(keys, [])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix')
@attr(assertion='non-printable prefix can be specified')
def test_bucket_list_prefix_unreadable():
    # FIX: shouldn't this test include strings that start with the tested prefix
    bucket = _create_keys(keys=['foo/bar', 'foo/baz', 'quux'])

    li = bucket.list(prefix='\x0a')
    eq(li.prefix, '\x0a')

    (keys, prefixes) = _get_keys_prefixes(li)
    eq(keys, [])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='returns only objects directly under prefix')
def test_bucket_list_prefix_delimiter_basic():
    bucket = _create_keys(keys=['foo/bar', 'foo/baz/xyzzy', 'quux/thud', 'asdf'])

    li = bucket.list(prefix='foo/', delimiter='/')
    eq(li.prefix, 'foo/')
    eq(li.delimiter, '/')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['foo/bar'])

    prefix_names = [e.name for e in prefixes]
    eq(prefix_names, ['foo/baz/'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='non-slash delimiters')
def test_bucket_list_prefix_delimiter_alt():
    bucket = _create_keys(keys=['bar', 'bazar', 'cab', 'foo'])

    li = bucket.list(prefix='ba', delimiter='a')
    eq(li.prefix, 'ba')
    eq(li.delimiter, 'a')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['bar'])

    prefix_names = [e.name for e in prefixes]
    eq(prefix_names, ['baza'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='finds nothing w/unmatched prefix')
def test_bucket_list_prefix_delimiter_prefix_not_exist():
    bucket = _create_keys(keys=['b/a/r', 'b/a/c', 'b/a/g', 'g'])

    li = bucket.list(prefix='d', delimiter='/')

    (keys, prefixes) = _get_keys_prefixes(li)
    eq(keys, [])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='over-ridden slash ceases to be a delimiter')
def test_bucket_list_prefix_delimiter_delimiter_not_exist():
    bucket = _create_keys(keys=['b/a/c', 'b/a/g', 'b/a/r', 'g'])

    li = bucket.list(prefix='b', delimiter='z')

    (keys, prefixes) = _get_keys_prefixes(li)
    names = [e.name for e in keys]
    eq(names, ['b/a/c', 'b/a/g', 'b/a/r'])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list under prefix w/delimiter')
@attr(assertion='finds nothing w/unmatched prefix and delimiter')
def test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist():
    bucket = _create_keys(keys=['b/a/c', 'b/a/g', 'b/a/r', 'g'])

    li = bucket.list(prefix='y', delimiter='z')

    (keys, prefixes) = _get_keys_prefixes(li)
    eq(keys, [])
    eq(prefixes, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=1, marker')
def test_bucket_list_maxkeys_one():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys(max_keys=1)
    eq(len(li), 1)
    eq(li.is_truncated, True)
    names = [e.name for e in li]
    eq(names, key_names[0:1])

    li = bucket.get_all_keys(marker=key_names[0])
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names[1:])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=0')
def test_bucket_list_maxkeys_zero():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    li = bucket.get_all_keys(max_keys=0)
    eq(li.is_truncated, False)
    eq(li, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/o max_keys')
def test_bucket_list_maxkeys_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys()
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names)
    eq(li.MaxKeys, '1000')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='invalid max_keys')
def test_bucket_list_maxkeys_invalid():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_all_keys, max_keys='blah')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidArgument')


@attr('fails_on_rgw')
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='non-printing max_keys')
def test_bucket_list_maxkeys_unreadable():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_all_keys, max_keys='\x0a')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    # Weird because you can clearly see an InvalidArgument error code. What's
    # also funny is the Amazon tells us that it's not an interger or within an
    # integer range. Is 'blah' in the integer range?
    eq(e.error_code, 'InvalidArgument')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='no pagination, no marker')
def test_bucket_list_marker_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys()
    eq(li.marker, '')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='no pagination, empty marker')
def test_bucket_list_marker_empty():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys(marker='')
    eq(li.marker, '')
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='non-printing marker')
def test_bucket_list_marker_unreadable():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys(marker='\x0a')
    eq(li.marker, '\x0a')
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='marker not-in-list')
def test_bucket_list_marker_not_in_list():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    li = bucket.get_all_keys(marker='blah')
    eq(li.marker, 'blah')
    names = [e.name for e in li]
    eq(names, ['foo', 'quxx'])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='marker after list')
def test_bucket_list_marker_after_list():
    bucket = _create_keys(keys=['bar', 'baz', 'foo', 'quxx'])

    li = bucket.get_all_keys(marker='zzz')
    eq(li.marker, 'zzz')
    eq(li.is_truncated, False)
    eq(li, [])


@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='marker before list')
def test_bucket_list_marker_before_list():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys(marker='aaa')
    eq(li.marker, 'aaa')
    eq(li.is_truncated, False)
    names = [e.name for e in li]
    eq(names, key_names)


def _compare_dates(iso_datetime, http_datetime):
    """
    compare an iso date and an http date, within an epsiolon
    """
    date = isodate.parse_datetime(iso_datetime)

    pd = email.utils.parsedate_tz(http_datetime)
    tz = isodate.tzinfo.FixedOffset(0, pd[-1]/60, 'who cares')
    date2 = datetime.datetime(*pd[:6], tzinfo=tz)

    # our tolerance
    minutes = 5
    acceptable_delta = datetime.timedelta(minutes=minutes)
    assert abs(date - date2) < acceptable_delta, \
            ("Times are not within {minutes} minutes of each other: "
             + "{date1!r}, {date2!r}"
             ).format(
                minutes=minutes,
                date1=iso_datetime,
                date2=http_datetime,
                )

@attr(resource='object')
@attr(method='head')
@attr(operation='compare w/bucket list')
@attr(assertion='return same metadata')
def test_bucket_list_return_data():
    key_names = ['bar', 'baz', 'foo']
    bucket = _create_keys(keys=key_names)

    # grab the data from each key individually
    data = {}
    for key_name in key_names:
        key = bucket.get_key(key_name)
        acl = key.get_acl()
        data.update({
            key_name: {
                'user_id': acl.owner.id,
                'display_name': acl.owner.display_name,
                'etag': key.etag,
                'last_modified': key.last_modified,
                'size': key.size,
                'md5': key.md5,
                'content_encoding': key.content_encoding,
                }
            })

    # now grab the data from each key through list
    li = bucket.list()
    for key in li:
        key_data = data[key.name]
        eq(key.content_encoding, key_data['content_encoding'])
        eq(key.owner.display_name, key_data['display_name'])
        eq(key.etag, key_data['etag'])
        eq(key.md5, key_data['md5'])
        eq(key.size, key_data['size'])
        eq(key.owner.id, key_data['user_id'])
        _compare_dates(key.last_modified, key_data['last_modified'])


@attr(resource='object.metadata')
@attr(method='head')
@attr(operation='modification-times')
@attr(assertion='http and ISO-6801 times agree')
def test_bucket_list_object_time():
    bucket = _create_keys(keys=['foo'])

    # Wed, 10 Aug 2011 21:58:25 GMT'
    key = bucket.get_key('foo')
    http_datetime = key.last_modified

    # ISO-6801 formatted datetime
    # there should be only one element, but list doesn't have a __getitem__
    # only an __iter__
    for key in bucket.list():
        iso_datetime = key.last_modified

    _compare_dates(iso_datetime, http_datetime)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all objects (anonymous)')
@attr(assertion='succeeds')
def test_bucket_list_objects_anonymous():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    conn = _create_connection_bad_auth()
    conn._auth_handler = AnonymousAuth.AnonymousAuthHandler(None, None, None) # Doesn't need this
    bucket = get_new_bucket()
    bucket.set_acl('public-read')
    anon_bucket = conn.get_bucket(bucket.name)
    anon_bucket.get_all_keys()

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all objects (anonymous)')
@attr(assertion='fails')
def test_bucket_list_objects_anonymous_fail():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    conn = _create_connection_bad_auth()
    conn._auth_handler = AnonymousAuth.AnonymousAuthHandler(None, None, None) # Doesn't need this
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3ResponseError, conn.get_bucket, bucket.name)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_bucket_notexist():
    # generate a (hopefully) unique, not-yet existent bucket name
    name = '{prefix}foo'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)

    e = assert_raises(boto.exception.S3ResponseError, s3.main.get_bucket, name)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


@attr(resource='bucket')
@attr(method='delete')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_bucket_delete_notexist():
    name = '{prefix}foo'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    e = assert_raises(boto.exception.S3ResponseError, s3.main.delete_bucket, name)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='bucket')
@attr(method='delete')
@attr(operation='non-empty bucket')
@attr(assertion='fails 409')
def test_bucket_delete_nonempty():
    bucket = get_new_bucket()

    # fill up bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('foocontent')

    # try to delete
    e = assert_raises(boto.exception.S3ResponseError, bucket.delete)
    eq(e.status, 409)
    eq(e.reason, 'Conflict')
    eq(e.error_code, 'BucketNotEmpty')

@attr(resource='object')
@attr(method='put')
@attr(operation='non-existant bucket')
@attr(assertion='fails 404')
def test_object_write_to_nonexist_bucket():
    name = '{prefix}foo'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    bucket = s3.main.get_bucket(name, validate=False)
    key = bucket.new_key('foo123bar')
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'foo')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


@attr(resource='bucket')
@attr(method='del')
@attr(operation='deleted bucket')
@attr(assertion='fails 404')
def test_bucket_create_delete():
    name = '{prefix}foo'.format(prefix=get_prefix())
    print 'Trying bucket {name!r}'.format(name=name)
    bucket = get_new_bucket(targets.main.default, name)
    # make sure it's actually there
    s3.main.get_bucket(bucket.name)
    bucket.delete()
    # make sure it's gone
    e = assert_raises(boto.exception.S3ResponseError, bucket.delete)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')


@attr(resource='object')
@attr(method='get')
@attr(operation='read contents that were never written')
@attr(assertion='fails 404')
def test_object_read_notexist():
    bucket = get_new_bucket()
    key = bucket.new_key('foobar')
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')

@attr(resource='object')
@attr(method='get')
@attr(operation='read contents that were never written to raise one error response')
@attr(assertion='RequestId appears in the error response')
def test_object_requestid_on_error():
    bucket = get_new_bucket()
    key = bucket.new_key('foobar')
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    request_id = re.search(r'<RequestId>.*</RequestId>', e.body.encode('utf-8')).group(0)
    assert request_id is not None

@attr(resource='object')
@attr(method='get')
@attr(operation='read contents that were never written to raise one error response')
@attr(assertion='RequestId in the error response matchs the x-amz-request-id in the headers')
def test_object_requestid_matchs_header_on_error():
    bucket = get_new_bucket()
    key = bucket.new_key('foobar')
    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    request_id = re.search(r'<RequestId>(.*)</RequestId>', e.body.encode('utf-8')).group(1)
    eq(key.resp.getheader('x-amz-request-id'), request_id)

# While the test itself passes, there's a SAX parser error during teardown. It
# seems to be a boto bug.  It happens with both amazon and dho.
# http://code.google.com/p/boto/issues/detail?id=501
@attr(resource='object')
@attr(method='put')
@attr(operation='write to non-printing key')
@attr(assertion='fails 404')
def test_object_create_unreadable():
    bucket = get_new_bucket()
    key = bucket.new_key('\x0a')
    key.set_contents_from_string('bar')


@attr(resource='object')
@attr(method='post')
@attr(operation='delete multiple objects')
@attr(assertion='deletes multiple objects with a single call')
def test_multi_object_delete():
	bucket = get_new_bucket()
	key0 = bucket.new_key('key0')
	key0.set_contents_from_string('foo')
	key1 = bucket.new_key('key1')
	key1.set_contents_from_string('bar')
	stored_keys = bucket.get_all_keys()
	eq(len(stored_keys), 2)
	result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 2)
        eq(len(result.errors), 0)
        eq(len(bucket.get_all_keys()), 0)

        # now remove again, should all succeed due to idempotency
        result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 2)
        eq(len(result.errors), 0)
        eq(len(bucket.get_all_keys()), 0)

@attr(resource='object')
@attr(method='put')
@attr(operation='write zero-byte key')
@attr(assertion='correct content length')
def test_object_head_zero_bytes():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('')

    key2 = bucket.get_key('foo')
    eq(key2.content_length, '0')

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct etag')
def test_object_write_check_etag():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    res = _make_request('PUT', bucket, key, body='bar', authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')
    eq(res.getheader("ETag"), '"37b51d194a7513e45b56f6524f2d51f2"')

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct cache control header')
def test_object_write_cache_control():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    cache_control = 'public, max-age=14400'
    key.set_contents_from_string('bar', headers = {'Cache-Control': cache_control})
    key2 = bucket.get_key('foo')
    eq(key2.cache_control, cache_control)

@attr(resource='object')
@attr(method='put')
@attr(operation='write key')
@attr(assertion='correct expires header')
def test_object_write_expires():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
    expires = expires.strftime("%a, %d %b %Y %H:%M:%S GMT")
    key.set_contents_from_string('bar', headers = {'Expires': expires})
    key2 = bucket.get_key('foo')
    eq(key2.expires, expires)

@attr(resource='object')
@attr(method='all')
@attr(operation='complete object life cycle')
@attr(assertion='read back what we wrote and rewrote')
def test_object_write_read_update_read_delete():
    bucket = get_new_bucket()
    # Write
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    # Read
    got = key.get_contents_as_string()
    eq(got, 'bar')
    # Update
    key.set_contents_from_string('soup')
    # Read
    got = key.get_contents_as_string()
    eq(got, 'soup')
    # Delete
    key.delete()


def _set_get_metadata(metadata, bucket=None):
    """
    create a new key in a (new or specified) bucket,
    set the meta1 property to a specified, value,
    and then re-read and return that property
    """
    if bucket is None:
        bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = ('foo')
    key.set_metadata('meta1', metadata)
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    return key2.get_metadata('meta1')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-read')
@attr(assertion='reread what we wrote')
def test_object_set_get_metadata_none_to_good():
    got = _set_get_metadata('mymeta')
    eq(got, 'mymeta')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-read')
@attr(assertion='write empty value, returns empty value')
def test_object_set_get_metadata_none_to_empty():
    got = _set_get_metadata('')
    eq(got, '')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='new value replaces old')
def test_object_set_get_metadata_overwrite_to_good():
    bucket = get_new_bucket()
    got = _set_get_metadata('oldmeta', bucket)
    eq(got, 'oldmeta')
    got = _set_get_metadata('newmeta', bucket)
    eq(got, 'newmeta')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='empty value replaces old')
def test_object_set_get_metadata_overwrite_to_empty():
    bucket = get_new_bucket()
    got = _set_get_metadata('oldmeta', bucket)
    eq(got, 'oldmeta')
    got = _set_get_metadata('', bucket)
    eq(got, '')


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='UTF-8 values passed through')
def test_object_set_get_unicode_metadata():
    bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = (u'foo')
    key.set_metadata('meta1', u"Hello World\xe9")
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    got = key2.get_metadata('meta1')
    eq(got, u"Hello World\xe9")


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/re-write')
@attr(assertion='non-UTF-8 values detected, but preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_non_utf8_metadata():
    bucket = get_new_bucket()
    key = boto.s3.key.Key(bucket)
    key.key = ('foo')
    key.set_metadata('meta1', '\x04mymeta')
    key.set_contents_from_string('bar')
    key2 = bucket.get_key('foo')
    got = key2.get_metadata('meta1')
    eq(got, '=?UTF-8?Q?=04mymeta?=')


def _set_get_metadata_unreadable(metadata, bucket=None):
    """
    set and then read back a meta-data value (which presumably
    includes some interesting characters), and return a list
    containing the stored value AND the encoding with which it
    was returned.
    """
    got = _set_get_metadata(metadata, bucket)
    got = decode_header(got)
    return got


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting prefixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_empty_to_unreadable_prefix():
    metadata = '\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting suffixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_empty_to_unreadable_suffix():
    metadata = 'h\x04'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write')
@attr(assertion='non-priting in-fixes noted and preserved')
def test_object_set_get_metadata_empty_to_unreadable_infix():
    metadata = 'h\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting prefixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_overwrite_to_unreadable_prefix():
    metadata = '\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = '\x05w'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting suffixes noted and preserved')
@attr('fails_strict_rfc2616')
def test_object_set_get_metadata_overwrite_to_unreadable_suffix():
    metadata = 'h\x04'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = 'h\x05'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata re-write')
@attr(assertion='non-priting in-fixes noted and preserved')
def test_object_set_get_metadata_overwrite_to_unreadable_infix():
    metadata = 'h\x04w'
    got = _set_get_metadata_unreadable(metadata)
    eq(got, [(metadata, 'utf-8')])
    metadata2 = 'h\x05w'
    got2 = _set_get_metadata_unreadable(metadata2)
    eq(got2, [(metadata2, 'utf-8')])


@attr(resource='object')
@attr(method='put')
@attr(operation='data re-write')
@attr(assertion='replaces previous metadata')
def test_object_metadata_replaced_on_put():
    bucket = get_new_bucket()

    # create object with metadata
    key = bucket.new_key('foo')
    key.set_metadata('meta1', 'bar')
    key.set_contents_from_string('bar')

    # overwrite previous object, no metadata
    key2 = bucket.new_key('foo')
    key2.set_contents_from_string('bar')

    # should see no metadata, as per 2nd write
    key3 = bucket.get_key('foo')
    got = key3.get_metadata('meta1')
    assert got is None, "did not expect to see metadata: %r" % got


@attr(resource='object')
@attr(method='put')
@attr(operation='data write from file (w/100-Continue)')
@attr(assertion='succeeds and returns written data')
def test_object_write_file():
    # boto Key.set_contents_from_file / .send_file uses Expect:
    # 100-Continue, so this test exercises that (though a bit too
    # subtly)
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    data = StringIO('bar')
    key.set_contents_from_file(fp=data)
    got = key.get_contents_as_string()
    eq(got, 'bar')


def _get_post_url(conn, bucket):

	url = '{protocol}://{host}:{port}/{bucket}'.format(protocol= 'https' if conn.is_secure else 'http',\
                    host=conn.host, port=conn.port, bucket=bucket.name)
	return url

@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_anonymous_request():
	bucket = get_new_bucket()
	url = _get_post_url(s3.main, bucket)
	bucket.set_acl('public-read-write')

	payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_authenticated_request():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')

@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request, bad access key')
@attr(assertion='fails')
def test_post_object_authenticated_request_bad_access_key():
	bucket = get_new_bucket()
	bucket.set_acl('public-read-write')

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , 'foo'),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds with status 201')
def test_post_object_set_success_code():
	bucket = get_new_bucket()
	bucket.set_acl('public-read-write')
	url = _get_post_url(s3.main, bucket)

	payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
	("success_action_status" , "201"),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 201)
	message = ET.fromstring(r.content).find('Key')
	eq(message.text,'foo.txt')


@attr(resource='object')
@attr(method='post')
@attr(operation='anonymous browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_set_invalid_success_code():
	bucket = get_new_bucket()
	bucket.set_acl('public-read-write')
	url = _get_post_url(s3.main, bucket)

	payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
	("success_action_status" , "404"),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	eq(r.content,'')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_upload_larger_than_chunk():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
	
	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 5*1024*1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	foo_string = 'foo' * 1024*1024

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', foo_string)])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, foo_string)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns written data')
def test_post_object_set_key_from_filename():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 5*1024*1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "${filename}"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('foo.txt', 'bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_ignored_header():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),("x-ignore-foo" , "bar"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with status 204')
def test_post_object_case_insensitive_condition_fields():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bUcKeT": bucket.name},\
	["StArTs-WiTh", "$KeY", "foo"],\
	{"AcL": "private"},\
	["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("kEy" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("aCl" , "private"),("signature" , signature),("pOLICy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds with escaped leading $ and returns written data')
def test_post_object_escaped_field_values():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("\$foo.txt")
	got = key.get_contents_as_string()
	eq(got, 'bar')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds and returns redirect url')
def test_post_object_success_redirect_action():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)
	redirect_url = _get_post_url(s3.main, bucket)
	bucket.set_acl('public-read')

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["eq", "$success_action_redirect", redirect_url],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),("success_action_redirect" , redirect_url),\
	('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 200)
	url = r.url
	key = bucket.get_key("foo.txt")
	eq(url,
	'{rurl}?bucket={bucket}&key={key}&etag=%22{etag}%22'.format(rurl = redirect_url, bucket = bucket.name,
	                                                             key = key.name, etag = key.etag.strip('"')))


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid signature error')
def test_post_object_invalid_signature():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())[::-1]

	payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with access key does not exist error')
def test_post_object_invalid_access_key():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id[::-1]),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid expiration error')
def test_post_object_invalid_date_format():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": str(expires),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with missing key error')
def test_post_object_no_key_specified():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with missing signature error')
def test_post_object_missing_signature():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "\$foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with extra input fields policy error')
def test_post_object_missing_policy_condition():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='succeeds using starts-with restriction on metadata header')
def test_post_object_user_specified_header():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
   ["starts-with", "$x-amz-meta-foo",  "bar"]
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('x-amz-meta-foo' , 'barclamp'),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 204)
	key = bucket.get_key("foo.txt")
	eq(key.get_metadata('foo'), 'barclamp')


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy condition failed error due to missing field in POST request')
def test_post_object_request_missing_policy_specified_field():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
   ["starts-with", "$x-amz-meta-foo",  "bar"]
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with conditions must be list error')
def test_post_object_condition_is_case_sensitive():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"CONDITIONS": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with expiration must be string error')
def test_post_object_expires_is_case_sensitive():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"EXPIRATION": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy expired error')
def test_post_object_expired_policy():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=-6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails using equality restriction on metadata header')
def test_post_object_invalid_request_field_value():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
   ["eq", "$x-amz-meta-foo",  ""]
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('x-amz-meta-foo' , 'barclamp'),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 403)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy missing expiration error')
def test_post_object_missing_expires_condition():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 1024],\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with policy missing conditions error')
def test_post_object_missing_conditions_list():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with allowable upload size exceeded error')
def test_post_object_upload_size_limit_exceeded():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0, 0]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid content length error')
def test_post_object_missing_content_length_argument():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 0]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with invalid JSON error')
def test_post_object_invalid_content_length_argument():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", -1, 0]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='post')
@attr(operation='authenticated browser based upload via POST request')
@attr(assertion='fails with upload size less than minimum allowable error')
def test_post_object_upload_size_below_minimum():
	bucket = get_new_bucket()

	url = _get_post_url(s3.main, bucket)

	utc = pytz.utc
	expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

	policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
	"conditions": [\
	{"bucket": bucket.name},\
	["starts-with", "$key", "foo"],\
	{"acl": "private"},\
	["starts-with", "$Content-Type", "text/plain"],\
	["content-length-range", 512, 1000]\
	]\
	}

	json_policy_document = json.JSONEncoder().encode(policy_document)
	policy = base64.b64encode(json_policy_document)
	conn = s3.main
	signature = base64.b64encode(hmac.new(conn.aws_secret_access_key, policy, sha).digest())

	payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , conn.aws_access_key_id),\
	("acl" , "private"),("signature" , signature),("policy" , policy),\
	("Content-Type" , "text/plain"),('file', ('bar'))])

	r = requests.post(url, files = payload)
	eq(r.status_code, 400)


@attr(resource='object')
@attr(method='put')
@attr(operation='data re-write w/ If-Match: the latest ETag')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifmatch_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    key.set_contents_from_string('zar', headers={'If-Match': key.etag.replace('"', '').strip()})
    got_new_data = key.get_contents_as_string()
    eq(got_new_data, 'zar')


@attr(resource='object')
@attr(method='put')
@attr(operation='data re-write w/ If-Match: outdated ETag')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifmatch_failed():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar',
                      headers={'If-Match': 'ABCORZ'})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    got_old_data = key.get_contents_as_string()
    eq(got_old_data, 'bar')


@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-Match: *')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifmatch_overwrite_existed_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    key.set_contents_from_string('zar', headers={'If-Match': '*'})
    got_new_data = key.get_contents_as_string()
    eq(got_new_data, 'zar')


@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite non-existing object w/ If-Match: *')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifmatch_nonexisted_failed():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar', headers={'If-Match': '*'})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    e = assert_raises(boto.exception.S3ResponseError, key.get_contents_as_string)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: outdated ETag')
@attr(assertion='replaces previous data and metadata')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    key.set_contents_from_string('zar', headers={'If-None-Match': 'ABCORZ'})
    got_new_data = key.get_contents_as_string()
    eq(got_new_data, 'zar')


@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: the latest ETag')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_failed():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'zar',
                      headers={'If-None-Match': key.etag.replace('"', '').strip()})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    got_old_data = key.get_contents_as_string()
    eq(got_old_data, 'bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite non-existing object w/ If-None-Match: *')
@attr(assertion='succeeds')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_nonexisted_good():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar', headers={'If-None-Match': '*'})
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')


@attr(resource='object')
@attr(method='put')
@attr(operation='overwrite existing object w/ If-None-Match: *')
@attr(assertion='fails 412')
@attr('fails_on_aws')
def test_put_object_ifnonmatch_overwrite_existed_failed():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    got_data = key.get_contents_as_string()
    eq(got_data, 'bar')

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string,
                      'zar', headers={'If-None-Match': '*'})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    got_old_data = key.get_contents_as_string()
    eq(got_old_data, 'bar')


def _setup_request(bucket_acl=None, object_acl=None):
    """
    add a foo key, and specified key and bucket acls to
    a (new or existing) bucket.
    """
    bucket = _create_keys(keys=['foo'])
    key = bucket.get_key('foo')

    if bucket_acl is not None:
        bucket.set_acl(bucket_acl)
    if object_acl is not None:
        key.set_acl(object_acl)

    return (bucket, key)

def _setup_bucket_request(bucket_acl=None):
    """
    set up a (new or existing) bucket with specified acl
    """
    bucket = get_new_bucket()

    if bucket_acl is not None:
        bucket.set_acl(bucket_acl)

    return bucket

def _make_request(method, bucket, key, body=None, authenticated=False, response_headers=None, expires_in=100000):
    """
    issue a request for a specified method, on a specified <bucket,key>,
    with a specified (optional) body (encrypted per the connection), and
    return the response (status, reason)
    """
    if authenticated:
        url = key.generate_url(expires_in, method=method, response_headers=response_headers)
        o = urlparse(url)
        path = o.path + '?' + o.query
    else:
        path = '/{bucket}/{obj}'.format(bucket=key.bucket.name, obj=key.name)

    if s3.main.is_secure:
        class_ = HTTPSConnection
    else:
        class_ = HTTPConnection

    c = class_(s3.main.host, s3.main.port, strict=True)
    c.request(method, path, body=body)
    res = c.getresponse()

    print res.status, res.reason
    return res

def _make_bucket_request(method, bucket, body=None, authenticated=False, expires_in=100000):
    """
    issue a request for a specified method, on a specified <bucket,key>,
    with a specified (optional) body (encrypted per the connection), and
    return the response (status, reason)
    """
    if authenticated:
        url = bucket.generate_url(expires_in, method=method)
        o = urlparse(url)
        path = o.path + '?' + o.query
    else:
        path = '/{bucket}'.format(bucket=bucket.name)

    if s3.main.is_secure:
        class_ = HTTPSConnection
    else:
        class_ = HTTPConnection

    c = class_(s3.main.host, s3.main.port, strict=True)
    c.request(method, path, body=body)
    res = c.getresponse()

    print res.status, res.reason
    return res


@attr(resource='object')
@attr(method='get')
@attr(operation='publically readable bucket')
@attr(assertion='bucket is readable')
def test_object_raw_get():
    (bucket, key) = _setup_request('public-read', 'public-read')
    res = _make_request('GET', bucket, key)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='get')
@attr(operation='deleted object and bucket')
@attr(assertion='fails 404')
def test_object_raw_get_bucket_gone():
    (bucket, key) = _setup_request('public-read', 'public-read')
    key.delete()
    bucket.delete()

    res = _make_request('GET', bucket, key)
    eq(res.status, 404)
    eq(res.reason, 'Not Found')


@attr(resource='object')
@attr(method='delete')
@attr(operation='deleted object and bucket')
@attr(assertion='fails 404')
def test_object_delete_key_bucket_gone():
    (bucket, key) = _setup_request()
    key.delete()
    bucket.delete()

    e = assert_raises(boto.exception.S3ResponseError, key.delete)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='get')
@attr(operation='deleted object')
@attr(assertion='fails 404')
def test_object_raw_get_object_gone():
    (bucket, key) = _setup_request('public-read', 'public-read')
    key.delete()

    res = _make_request('GET', bucket, key)
    eq(res.status, 404)
    eq(res.reason, 'Not Found')

def _head_bucket(bucket, authenticated=True):
    res = _make_bucket_request('HEAD', bucket, authenticated=authenticated)
    eq(res.status, 200)
    eq(res.reason, 'OK')

    result = {}

    obj_count = res.getheader('x-rgw-object-count')
    if obj_count != None:
        result['x-rgw-object-count'] = int(obj_count)

    bytes_used = res.getheader('x-rgw-bytes-used')
    if bytes_used is not None:
        result['x-rgw-bytes-used'] = int(bytes_used)

    return result


@attr(resource='bucket')
@attr(method='head')
@attr(operation='head bucket')
@attr(assertion='succeeds')
def test_bucket_head():
    bucket = get_new_bucket()

    _head_bucket(bucket)


# This test relies on Ceph extensions.
# http://tracker.ceph.com/issues/2313
@attr('fails_on_aws')
@attr(resource='bucket')
@attr(method='head')
@attr(operation='read bucket extended information')
@attr(assertion='extended information is getting updated')
def test_bucket_head_extended():
    bucket = get_new_bucket()

    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 0), 0)
    eq(result.get('x-rgw-bytes-used', 0), 0)

    _create_keys(bucket, keys=['foo', 'bar', 'baz'])

    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 3), 3)

    assert result.get('x-rgw-bytes-used', 9) > 0


@attr(resource='bucket.acl')
@attr(method='get')
@attr(operation='unauthenticated on private bucket')
@attr(assertion='succeeds')
def test_object_raw_get_bucket_acl():
    (bucket, key) = _setup_request('private', 'public-read')

    res = _make_request('GET', bucket, key)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object.acl')
@attr(method='get')
@attr(operation='unauthenticated on private object')
@attr(assertion='fails 403')
def test_object_raw_get_object_acl():
    (bucket, key) = _setup_request('public-read', 'private')

    res = _make_request('GET', bucket, key)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on public bucket/object')
@attr(assertion='succeeds')
def test_object_raw_authenticated():
    (bucket, key) = _setup_request('public-read', 'public-read')

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on private bucket/private object with modified response headers')
@attr(assertion='succeeds')
@attr('fails_on_rgw')
def test_object_raw_response_headers():
    (bucket, key) = _setup_request('private', 'private')

    response_headers = {
            'response-content-type': 'foo/bar',
            'response-content-disposition': 'bla',
            'response-content-language': 'esperanto',
            'response-content-encoding': 'aaa',
            'response-expires': '123',
            'response-cache-control': 'no-cache',
        }

    res = _make_request('GET', bucket, key, authenticated=True,
                        response_headers=response_headers)
    eq(res.status, 200)
    eq(res.reason, 'OK')
    eq(res.getheader('content-type'), 'foo/bar')
    eq(res.getheader('content-disposition'), 'bla')
    eq(res.getheader('content-language'), 'esperanto')
    eq(res.getheader('content-encoding'), 'aaa')
    eq(res.getheader('expires'), '123')
    eq(res.getheader('cache-control'), 'no-cache')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on private bucket/public object')
@attr(assertion='succeeds')
def test_object_raw_authenticated_bucket_acl():
    (bucket, key) = _setup_request('private', 'public-read')

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='authenticated on public bucket/private object')
@attr(assertion='succeeds')
def test_object_raw_authenticated_object_acl():
    (bucket, key) = _setup_request('public-read', 'private')

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on deleted object and bucket')
@attr(assertion='fails 404')
def test_object_raw_authenticated_bucket_gone():
    (bucket, key) = _setup_request('public-read', 'public-read')
    key.delete()
    bucket.delete()

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 404)
    eq(res.reason, 'Not Found')


@attr(resource='object')
@attr(method='get')
@attr(operation='authenticated on deleted object')
@attr(assertion='fails 404')
def test_object_raw_authenticated_object_gone():
    (bucket, key) = _setup_request('public-read', 'public-read')
    key.delete()

    res = _make_request('GET', bucket, key, authenticated=True)
    eq(res.status, 404)
    eq(res.reason, 'Not Found')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='get')
@attr(operation='x-amz-expires check not expired')
@attr(assertion='succeeds')
def test_object_raw_get_x_amz_expires_not_expired():
    check_aws4_support()
    (bucket, key) = _setup_request('public-read', 'public-read')

    res = _make_request('GET', bucket, key, authenticated=True, expires_in=100000)
    eq(res.status, 200)


@tag('auth_aws4')
@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of range zero')
@attr(assertion='fails 403')
def test_object_raw_get_x_amz_expires_out_range_zero():
    check_aws4_support()
    (bucket, key) = _setup_request('public-read', 'public-read')

    res = _make_request('GET', bucket, key, authenticated=True, expires_in=0)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of max range')
@attr(assertion='fails 403')
def test_object_raw_get_x_amz_expires_out_max_range():
    check_aws4_support()
    (bucket, key) = _setup_request('public-read', 'public-read')

    res = _make_request('GET', bucket, key, authenticated=True, expires_in=604801)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='get')
@attr(operation='check x-amz-expires value out of positive range')
@attr(assertion='succeeds')
def test_object_raw_get_x_amz_expires_out_positive_range():
    check_aws4_support()
    (bucket, key) = _setup_request('public-read', 'public-read')

    res = _make_request('GET', bucket, key, authenticated=True, expires_in=-7)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@attr(resource='object')
@attr(method='put')
@attr(operation='unauthenticated, no object acls')
@attr(assertion='fails 403')
def test_object_raw_put():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')

    res = _make_request('PUT', bucket, key, body='foo')
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


@attr(resource='object')
@attr(method='put')
@attr(operation='unauthenticated, publically writable object')
@attr(assertion='succeeds')
def test_object_raw_put_write_access():
    bucket = get_new_bucket()
    bucket.set_acl('public-read-write')
    key = bucket.new_key('foo')

    res = _make_request('PUT', bucket, key, body='foo')
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='put')
@attr(operation='authenticated, no object acls')
@attr(assertion='succeeds')
def test_object_raw_put_authenticated():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')

    res = _make_request('PUT', bucket, key, body='foo', authenticated=True)
    eq(res.status, 200)
    eq(res.reason, 'OK')


@attr(resource='object')
@attr(method='put')
@attr(operation='authenticated, no object acls')
@attr(assertion='succeeds')
def test_object_raw_put_authenticated_expired():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')

    res = _make_request('PUT', bucket, key, body='foo', authenticated=True, expires_in=-1000)
    eq(res.status, 403)
    eq(res.reason, 'Forbidden')


def check_bad_bucket_name(name):
    """
    Attempt to create a bucket with a specified name, and confirm
    that the request fails because of an invalid bucket name.
    """
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, targets.main.default, name)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidBucketName')


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@attr('fails_on_aws')
# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='name begins with underscore')
@attr(assertion='fails with subdomain: 400')
def test_bucket_create_naming_bad_starts_nonalpha():
    bucket_name = get_new_bucket_name()
    check_bad_bucket_name('_' + bucket_name)


@attr(resource='bucket')
@attr(method='put')
@attr(operation='empty name')
@attr(assertion='fails 405')
def test_bucket_create_naming_bad_short_empty():
    # bucket creates where name is empty look like PUTs to the parent
    # resource (with slash), hence their error response is different
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, targets.main.default, '')
    eq(e.status, 405)
    eq(e.reason, 'Method Not Allowed')
    eq(e.error_code, 'MethodNotAllowed')


@attr(resource='bucket')
@attr(method='put')
@attr(operation='short (one character) name')
@attr(assertion='fails 400')
def test_bucket_create_naming_bad_short_one():
    check_bad_bucket_name('a')


@attr(resource='bucket')
@attr(method='put')
@attr(operation='short (two character) name')
@attr(assertion='fails 400')
def test_bucket_create_naming_bad_short_two():
    check_bad_bucket_name('aa')

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='excessively long names')
@attr(assertion='fails with subdomain: 400')
def test_bucket_create_naming_bad_long():
    check_bad_bucket_name(256*'a')
    check_bad_bucket_name(280*'a')
    check_bad_bucket_name(3000*'a')


def check_good_bucket_name(name, _prefix=None):
    """
    Attempt to create a bucket with a specified name
    and (specified or default) prefix, returning the
    results of that effort.
    """
    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    if _prefix is None:
        _prefix = get_prefix()
    get_new_bucket(targets.main.default, '{prefix}{name}'.format(
            prefix=_prefix,
            name=name,
            ))


def _test_bucket_create_naming_good_long(length):
    """
    Attempt to create a bucket whose name (including the
    prefix) is of a specified length.
    """
    prefix = get_new_bucket_name()
    assert len(prefix) < 255
    num = length - len(prefix)
    get_new_bucket(targets.main.default, '{prefix}{name}'.format(
            prefix=prefix,
            name=num*'a',
            ))


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/250 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_250():
    _test_bucket_create_naming_good_long(250)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/251 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_251():
    _test_bucket_create_naming_good_long(251)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/252 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_252():
    _test_bucket_create_naming_good_long(252)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/253 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_253():
    _test_bucket_create_naming_good_long(253)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/254 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_254():
    _test_bucket_create_naming_good_long(254)


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/255 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_good_long_255():
    _test_bucket_create_naming_good_long(255)

# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='get')
@attr(operation='list w/251 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_list_long_name():
    prefix = get_new_bucket_name()
    length = 251
    num = length - len(prefix)
    bucket = get_new_bucket(targets.main.default, '{prefix}{name}'.format(
            prefix=prefix,
            name=num*'a',
            ))
    got = bucket.list()
    got = list(got)
    eq(got, [])


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@attr('fails_on_aws')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/ip address for name')
@attr(assertion='fails on aws')
def test_bucket_create_naming_bad_ip():
    check_bad_bucket_name('192.168.5.123')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/! in name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_bad_punctuation():
    # characters other than [a-zA-Z0-9._-]
    check_bad_bucket_name('alpha!soup')


# test_bucket_create_naming_dns_* are valid but not recommended
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/underscore in name')
@attr(assertion='succeeds')
def test_bucket_create_naming_dns_underscore():
    check_good_bucket_name('foo_bar')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/100 byte name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_dns_long():
    prefix = get_prefix()
    assert len(prefix) < 50
    num = 100 - len(prefix)
    check_good_bucket_name(num * 'a')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/dash at end of name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_dns_dash_at_end():
    check_good_bucket_name('foo-')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/.. in name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_dns_dot_dot():
    check_good_bucket_name('foo..bar')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/.- in name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_dns_dot_dash():
    check_good_bucket_name('foo.-bar')


# Breaks DNS with SubdomainCallingFormat
@attr('fails_with_subdomain')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/-. in name')
@attr(assertion='fails with subdomain')
def test_bucket_create_naming_dns_dash_dot():
    check_good_bucket_name('foo-.bar')


@attr(resource='bucket')
@attr(method='put')
@attr(operation='re-create')
def test_bucket_create_exists():
    # aws-s3 default region allows recreation of buckets
    # but all other regions fail with BucketAlreadyOwnedByYou.
    bucket = get_new_bucket(targets.main.default)
    try:
        get_new_bucket(targets.main.default, bucket.name)
    except boto.exception.S3CreateError, e:
        eq(e.status, 409)
        eq(e.reason, 'Conflict')
        eq(e.error_code, 'BucketAlreadyOwnedByYou')


@attr(resource='bucket')
@attr(method='get')
@attr(operation='get location')
def test_bucket_get_location():
    bucket = get_new_bucket(targets.main.default)
    actual_location = bucket.get_location()
    expected_location = targets.main.default.conf.api_name
    eq(actual_location, expected_location)


@attr(resource='bucket')
@attr(method='put')
@attr(operation='re-create by non-owner')
@attr(assertion='fails 409')
def test_bucket_create_exists_nonowner():
    # Names are shared across a global namespace. As such, no two
    # users can create a bucket with that same name.
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3CreateError, get_new_bucket, targets.alt.default, bucket.name)
    eq(e.status, 409)
    eq(e.reason, 'Conflict')
    eq(e.error_code, 'BucketAlreadyExists')


@attr(resource='bucket')
@attr(method='del')
@attr(operation='delete by non-owner')
@attr(assertion='fails')
def test_bucket_delete_nonowner():
    bucket = get_new_bucket()
    check_access_denied(s3.alt.delete_bucket, bucket.name)


@attr(resource='bucket')
@attr(method='get')
@attr(operation='default acl')
@attr(assertion='read back expected defaults')
def test_bucket_acl_default():
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    print repr(policy)
    eq(policy.owner.type, None)
    eq(policy.owner.id, config.main.user_id)
    eq(policy.owner.display_name, config.main.display_name)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='bucket')
@attr(method='get')
@attr(operation='public-read acl')
@attr(assertion='read back expected defaults')
def test_bucket_acl_canned_during_create():
    name = get_new_bucket_name()
    bucket = targets.main.default.connection.create_bucket(name, policy = 'public-read')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

@attr(resource='bucket')
@attr(method='put')
@attr(operation='acl: public-read,private')
@attr(assertion='read back expected values')
def test_bucket_acl_canned():
    bucket = get_new_bucket()
    # Since it defaults to private, set it public-read first
    bucket.set_acl('public-read')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

    # Then back to private.
    bucket.set_acl('private')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='bucket.acls')
@attr(method='put')
@attr(operation='acl: public-read-write')
@attr(assertion='read back expected values')
def test_bucket_acl_canned_publicreadwrite():
    bucket = get_new_bucket()
    bucket.set_acl('public-read-write')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            dict(
                permission='WRITE',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='bucket')
@attr(method='put')
@attr(operation='acl: authenticated-read')
@attr(assertion='read back expected values')
def test_bucket_acl_canned_authenticatedread():
    bucket = get_new_bucket()
    bucket.set_acl('authenticated-read')
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='get')
@attr(operation='default acl')
@attr(assertion='read back expected defaults')
def test_object_acl_default():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl public-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_during_create():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar', policy='public-read')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl public-read,private')
@attr(assertion='read back expected values')
def test_object_acl_canned():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    # Since it defaults to private, set it public-read first
    key.set_acl('public-read')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

    # Then back to private.
    key.set_acl('private')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='object')
@attr(method='put')
@attr(operation='acl public-read-write')
@attr(assertion='read back expected values')
def test_object_acl_canned_publicreadwrite():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_acl('public-read-write')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            dict(
                permission='WRITE',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl authenticated-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_authenticatedread():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_acl('authenticated-read')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl bucket-owner-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_bucketownerread():
    bucket = get_new_bucket(targets.main.default)
    bucket.set_acl('public-read-write')

    key = s3.alt.get_bucket(bucket.name).new_key('foo')
    key.set_contents_from_string('bar')

    bucket_policy = bucket.get_acl()
    bucket_owner_id = bucket_policy.owner.id
    bucket_owner_display = bucket_policy.owner.display_name

    key.set_acl('bucket-owner-read')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=bucket_owner_id,
                display_name=bucket_owner_display,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    key.delete()
    bucket.delete()


@attr(resource='object.acls')
@attr(method='put')
@attr(operation='acl bucket-owner-read')
@attr(assertion='read back expected values')
def test_object_acl_canned_bucketownerfullcontrol():
    bucket = get_new_bucket(targets.main.default)
    bucket.set_acl('public-read-write')

    key = s3.alt.get_bucket(bucket.name).new_key('foo')
    key.set_contents_from_string('bar')

    bucket_policy = bucket.get_acl()
    bucket_owner_id = bucket_policy.owner.id
    bucket_owner_display = bucket_policy.owner.display_name

    key.set_acl('bucket-owner-full-control')
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='FULL_CONTROL',
                id=bucket_owner_id,
                display_name=bucket_owner_display,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    key.delete()
    bucket.delete()

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='set write-acp')
@attr(assertion='does not modify owner')
def test_object_acl_full_control_verify_owner():
    bucket = get_new_bucket(targets.main.default)
    bucket.set_acl('public-read-write')

    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    key.add_user_grant(permission='FULL_CONTROL', user_id=config.alt.user_id)

    k2 = s3.alt.get_bucket(bucket.name).get_key('foo')

    k2.add_user_grant(permission='READ_ACP', user_id=config.alt.user_id)

    policy = k2.get_acl()
    eq(policy.owner.id, config.main.user_id)

@attr(resource='object.acls')
@attr(method='put')
@attr(operation='set write-acp')
@attr(assertion='does not modify other attributes')
def test_object_acl_full_control_verify_attributes():
    bucket = get_new_bucket(targets.main.default)
    bucket.set_acl('public-read-write')

    key = bucket.new_key('foo')
    key.set_contents_from_string('bar', {'x-amz-foo': 'bar'})

    etag = key.etag
    content_type = key.content_type

    for k in bucket.list():
        eq(k.etag, etag)
        eq(k.content_type, content_type)

    key.add_user_grant(permission='FULL_CONTROL', user_id=config.alt.user_id)

    for k in bucket.list():
        eq(k.etag, etag)
        eq(k.content_type, content_type)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl private')
@attr(assertion='a private object can be set to private')
def test_bucket_acl_canned_private_to_private():
    bucket = get_new_bucket()
    bucket.set_acl('private')


def _make_acl_xml(acl):
    """
    Return the xml form of an ACL entry
    """
    return '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner>' + acl.to_xml() + '</AccessControlPolicy>'


def _build_bucket_acl_xml(permission, bucket=None):
    """
    add the specified permission for the current user to
    a (new or specified) bucket, in XML form, set it, and
    then read it back to confirm it was correctly set
    """
    acl = boto.s3.acl.ACL()
    acl.add_user_grant(permission=permission, user_id=config.main.user_id)
    XML = _make_acl_xml(acl)
    if bucket is None:
        bucket = get_new_bucket()
    bucket.set_xml_acl(XML)
    policy = bucket.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission=permission,
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl FULL_CONTROL (xml)')
@attr(assertion='reads back correctly')
def test_bucket_acl_xml_fullcontrol():
    _build_bucket_acl_xml('FULL_CONTROL')


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl WRITE (xml)')
@attr(assertion='reads back correctly')
def test_bucket_acl_xml_write():
    _build_bucket_acl_xml('WRITE')


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl WRITE_ACP (xml)')
@attr(assertion='reads back correctly')
def test_bucket_acl_xml_writeacp():
    _build_bucket_acl_xml('WRITE_ACP')


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl READ (xml)')
@attr(assertion='reads back correctly')
def test_bucket_acl_xml_read():
    _build_bucket_acl_xml('READ')


@attr(resource='bucket.acls')
@attr(method='ACLs')
@attr(operation='set acl READ_ACP (xml)')
@attr(assertion='reads back correctly')
def test_bucket_acl_xml_readacp():
    _build_bucket_acl_xml('READ_ACP')


def _build_object_acl_xml(permission):
    """
    add the specified permission for the current user to
    a new object in a new bucket, in XML form, set it, and
    then read it back to confirm it was correctly set
    """
    acl = boto.s3.acl.ACL()
    acl.add_user_grant(permission=permission, user_id=config.main.user_id)
    XML = _make_acl_xml(acl)
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(XML)
    policy = key.get_acl()
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission=permission,
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl FULL_CONTROL (xml)')
@attr(assertion='reads back correctly')
def test_object_acl_xml():
    _build_object_acl_xml('FULL_CONTROL')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl WRITE (xml)')
@attr(assertion='reads back correctly')
def test_object_acl_xml_write():
    _build_object_acl_xml('WRITE')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl WRITE_ACP (xml)')
@attr(assertion='reads back correctly')
def test_object_acl_xml_writeacp():
    _build_object_acl_xml('WRITE_ACP')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl READ (xml)')
@attr(assertion='reads back correctly')
def test_object_acl_xml_read():
    _build_object_acl_xml('READ')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set acl READ_ACP (xml)')
@attr(assertion='reads back correctly')
def test_object_acl_xml_readacp():
    _build_object_acl_xml('READ_ACP')


def _bucket_acl_grant_userid(permission):
    """
    create a new bucket, grant a specific user the specified
    permission, read back the acl and verify correct setting
    """
    bucket = get_new_bucket()
    # add alt user
    policy = bucket.get_acl()
    policy.acl.add_user_grant(permission, config.alt.user_id)
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission=permission,
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    return bucket


def _check_bucket_acl_grant_can_read(bucket):
    """
    verify ability to read the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name)


def _check_bucket_acl_grant_cant_read(bucket):
    """
    verify inability to read the specified bucket
    """
    check_access_denied(s3.alt.get_bucket, bucket.name)


def _check_bucket_acl_grant_can_readacp(bucket):
    """
    verify ability to read acls on specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    bucket2.get_acl()


def _check_bucket_acl_grant_cant_readacp(bucket):
    """
    verify inability to read acls on specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    check_access_denied(bucket2.get_acl)


def _check_bucket_acl_grant_can_write(bucket):
    """
    verify ability to write the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    key = bucket2.new_key('foo-write')
    key.set_contents_from_string('bar')


def _check_bucket_acl_grant_cant_write(bucket):
    """
    verify inability to write the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    key = bucket2.new_key('foo-write')
    check_access_denied(key.set_contents_from_string, 'bar')


def _check_bucket_acl_grant_can_writeacp(bucket):
    """
    verify ability to set acls on the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    bucket2.set_acl('public-read')


def _check_bucket_acl_grant_cant_writeacp(bucket):
    """
    verify inability to set acls on the specified bucket
    """
    bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    check_access_denied(bucket2.set_acl, 'public-read')


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid FULL_CONTROL')
@attr(assertion='can read/write data/acls')
def test_bucket_acl_grant_userid_fullcontrol():
    bucket = _bucket_acl_grant_userid('FULL_CONTROL')

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket)
    # can write
    _check_bucket_acl_grant_can_write(bucket)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket)

    # verify owner did not change
    bucket2 = s3.main.get_bucket(bucket.name)
    policy = bucket2.get_acl()
    eq(policy.owner.id, config.main.user_id)
    eq(policy.owner.display_name, config.main.display_name)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid READ')
@attr(assertion='can read data, no other r/w')
def test_bucket_acl_grant_userid_read():
    bucket = _bucket_acl_grant_userid('READ')

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid READ_ACP')
@attr(assertion='can read acl, no other r/w')
def test_bucket_acl_grant_userid_readacp():
    bucket = _bucket_acl_grant_userid('READ_ACP')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can't write acp
    #_check_bucket_acl_grant_cant_writeacp_can_readacp(bucket)
    _check_bucket_acl_grant_cant_writeacp(bucket)

@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid WRITE')
@attr(assertion='can write data, no other r/w')
def test_bucket_acl_grant_userid_write():
    bucket = _bucket_acl_grant_userid('WRITE')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can write
    _check_bucket_acl_grant_can_write(bucket)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/userid WRITE_ACP')
@attr(assertion='can write acls, no other r/w')
def test_bucket_acl_grant_userid_writeacp():
    bucket = _bucket_acl_grant_userid('WRITE_ACP')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket)


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='set acl w/invalid userid')
@attr(assertion='fails 400')
def test_bucket_acl_grant_nonexist_user():
    bucket = get_new_bucket()
    # add alt user
    bad_user_id = '_foo'
    policy = bucket.get_acl()
    policy.acl.add_user_grant('FULL_CONTROL', bad_user_id)
    print policy.to_xml()
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_acl, policy)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidArgument')


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='revoke all ACLs')
@attr(assertion='can: read obj, get/set bucket acl, cannot write objs')
def test_bucket_acl_no_grants():
    bucket = get_new_bucket()

    # write content to the bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    # clear grants
    policy = bucket.get_acl()
    policy.acl.grants = []

    # remove read/write permission
    bucket.set_acl(policy)

    # can read
    bucket.get_key('foo')

    # can't write
    key = bucket.new_key('baz')
    check_access_denied(key.set_contents_from_string, 'bar')

    # can read acl
    bucket.get_acl()

    # can write acl
    bucket.set_acl('private')

def _get_acl_header(user=None, perms=None):
    all_headers = ["read", "write", "read-acp", "write-acp", "full-control"]
    headers = {}

    if user == None:
        user = config.alt.user_id

    if perms != None:
        for perm in perms:
           headers["x-amz-grant-{perm}".format(perm=perm)] = "id={uid}".format(uid=user)

    else:
        for perm in all_headers:
            headers["x-amz-grant-{perm}".format(perm=perm)] = "id={uid}".format(uid=user)

    return headers

@attr(resource='object')
@attr(method='PUT')
@attr(operation='add all grants to user through headers')
@attr(assertion='adds all grants individually to second user')
@attr('fails_on_dho')
def test_object_header_acl_grants():
    bucket = get_new_bucket()
    headers = _get_acl_header()
    k = bucket.new_key("foo_key")
    k.set_contents_from_string("bar", headers=headers)

    policy = k.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='READ',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='WRITE',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ_ACP',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='WRITE_ACP',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='FULL_CONTROL',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )


@attr(resource='bucket')
@attr(method='PUT')
@attr(operation='add all grants to user through headers')
@attr(assertion='adds all grants individually to second user')
@attr('fails_on_dho')
def test_bucket_header_acl_grants():
    headers = _get_acl_header()
    bucket = get_new_bucket(targets.main.default, get_prefix(), headers)

    policy = bucket.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='READ',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='WRITE',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ_ACP',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='WRITE_ACP',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='FULL_CONTROL',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    # alt user can write
    bucket2 = s3.alt.get_bucket(bucket.name)
    key = bucket2.new_key('foo')
    key.set_contents_from_string('bar')


# This test will fail on DH Objects. DHO allows multiple users with one account, which
# would violate the uniqueness requirement of a user's email. As such, DHO users are
# created without an email.
@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='add second FULL_CONTROL user')
@attr(assertion='works for S3, fails for DHO')
def test_bucket_acl_grant_email():
    bucket = get_new_bucket()
    # add alt user
    policy = bucket.get_acl()
    policy.acl.add_email_grant('FULL_CONTROL', config.alt.email)
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='FULL_CONTROL',
                id=config.alt.user_id,
                display_name=config.alt.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ],
        )

    # alt user can write
    bucket2 = s3.alt.get_bucket(bucket.name)
    key = bucket2.new_key('foo')
    key.set_contents_from_string('bar')


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='add acl for nonexistent user')
@attr(assertion='fail 400')
def test_bucket_acl_grant_email_notexist():
    # behavior not documented by amazon
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    policy.acl.add_email_grant('FULL_CONTROL', NONEXISTENT_EMAIL)
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_acl, policy)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'UnresolvableGrantByEmailAddress')


@attr(resource='bucket')
@attr(method='ACLs')
@attr(operation='revoke all ACLs')
@attr(assertion='acls read back as empty')
def test_bucket_acl_revoke_all():
    # revoke all access, including the owner's access
    bucket = get_new_bucket()
    policy = bucket.get_acl()
    policy.acl.grants = []
    bucket.set_acl(policy)
    policy = bucket.get_acl()
    eq(len(policy.acl.grants), 0)


# TODO rgw log_bucket.set_as_logging_target() gives 403 Forbidden
# http://tracker.newdream.net/issues/984
@attr(resource='bucket.log')
@attr(method='put')
@attr(operation='set/enable/disable logging target')
@attr(assertion='operations succeed')
@attr('fails_on_rgw')
def test_logging_toggle():
    bucket = get_new_bucket()
    log_bucket = get_new_bucket(targets.main.default, bucket.name + '-log')
    log_bucket.set_as_logging_target()
    bucket.enable_logging(target_bucket=log_bucket, target_prefix=bucket.name)
    bucket.disable_logging()
    # NOTE: this does not actually test whether or not logging works


def _setup_access(bucket_acl, object_acl):
    """
    Simple test fixture: create a bucket with given ACL, with objects:
    - a: owning user, given ACL
    - a2: same object accessed by some other user
    - b: owning user, default ACL in bucket w/given ACL
    - b2: same object accessed by a some other user
    """
    obj = bunch.Bunch()
    bucket = get_new_bucket()
    bucket.set_acl(bucket_acl)
    obj.a = bucket.new_key('foo')
    obj.a.set_contents_from_string('foocontent')
    obj.a.set_acl(object_acl)
    obj.b = bucket.new_key('bar')
    obj.b.set_contents_from_string('barcontent')

    # bucket2 is being accessed by a different user
    obj.bucket2 = s3.alt.get_bucket(bucket.name, validate=False)
    obj.a2 = obj.bucket2.new_key(obj.a.name)
    obj.b2 = obj.bucket2.new_key(obj.b.name)
    obj.new = obj.bucket2.new_key('new')

    return obj


def get_bucket_key_names(bucket):
    return frozenset(k.name for k in bucket.list())


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: private/private')
@attr(assertion='public has no access to bucket or objects')
def test_access_bucket_private_object_private():
    # all the test_access_* tests follow this template
    obj = _setup_access(bucket_acl='private', object_acl='private')
    # a should be public-read, b gets default (private)
    # acled object read fail
    check_access_denied(obj.a2.get_contents_as_string)
    # acled object write fail
    check_access_denied(obj.a2.set_contents_from_string, 'barcontent')
    # default object read fail
    check_access_denied(obj.b2.get_contents_as_string)
    # default object write fail
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    # bucket read fail
    check_access_denied(get_bucket_key_names, obj.bucket2)
    # bucket write fail
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: private/public-read')
@attr(assertion='public can only read readable object')
def test_access_bucket_private_object_publicread():
    obj = _setup_access(bucket_acl='private', object_acl='public-read')
    # a should be public-read, b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    check_access_denied(get_bucket_key_names, obj.bucket2)
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: private/public-read/write')
@attr(assertion='public can only read the readable object')
def test_access_bucket_private_object_publicreadwrite():
    obj = _setup_access(bucket_acl='private', object_acl='public-read-write')
    # a should be public-read-only ... because it is in a private bucket
    # b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    check_access_denied(get_bucket_key_names, obj.bucket2)
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read/private')
@attr(assertion='public can only list the bucket')
def test_access_bucket_publicread_object_private():
    obj = _setup_access(bucket_acl='public-read', object_acl='private')
    # a should be private, b gets default (private)
    check_access_denied(obj.a2.get_contents_as_string)
    check_access_denied(obj.a2.set_contents_from_string, 'barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read/public-read')
@attr(assertion='public can read readable objects and list bucket')
def test_access_bucket_publicread_object_publicread():
    obj = _setup_access(bucket_acl='public-read', object_acl='public-read')
    # a should be public-read, b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read/public-read-write')
@attr(assertion='public can read readable objects and list bucket')
def test_access_bucket_publicread_object_publicreadwrite():
    obj = _setup_access(bucket_acl='public-read', object_acl='public-read-write')
    # a should be public-read-only ... because it is in a r/o bucket
    # b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    check_access_denied(obj.a2.set_contents_from_string, 'foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    check_access_denied(obj.b2.set_contents_from_string, 'baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    check_access_denied(obj.new.set_contents_from_string, 'newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read-write/private')
@attr(assertion='private objects cannot be read, but can be overwritten')
def test_access_bucket_publicreadwrite_object_private():
    obj = _setup_access(bucket_acl='public-read-write', object_acl='private')
    # a should be private, b gets default (private)
    check_access_denied(obj.a2.get_contents_as_string)
    obj.a2.set_contents_from_string('barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')


@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read-write/public-read')
@attr(assertion='private objects cannot be read, but can be overwritten')
def test_access_bucket_publicreadwrite_object_publicread():
    obj = _setup_access(bucket_acl='public-read-write', object_acl='public-read')
    # a should be public-read, b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    obj.a2.set_contents_from_string('barcontent')
    check_access_denied(obj.b2.get_contents_as_string)
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')

@attr(resource='object')
@attr(method='ACLs')
@attr(operation='set bucket/object acls: public-read-write/public-read-write')
@attr(assertion='private objects cannot be read, but can be overwritten')
def test_access_bucket_publicreadwrite_object_publicreadwrite():
    obj = _setup_access(bucket_acl='public-read-write', object_acl='public-read-write')
    # a should be public-read-write, b gets default (private)
    eq(obj.a2.get_contents_as_string(), 'foocontent')
    obj.a2.set_contents_from_string('foooverwrite')
    check_access_denied(obj.b2.get_contents_as_string)
    obj.b2.set_contents_from_string('baroverwrite')
    eq(get_bucket_key_names(obj.bucket2), frozenset(['foo', 'bar']))
    obj.new.set_contents_from_string('newcontent')

@attr(resource='object')
@attr(method='put')
@attr(operation='set object acls')
@attr(assertion='valid XML ACL sets properly')
def test_object_set_valid_acl():
    XML_1 = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.main.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(XML_1)

@attr(resource='object')
@attr(method='put')
@attr(operation='set object acls')
@attr(assertion='invalid XML ACL fails 403')
def test_object_giveaway():
    CORRECT_ACL = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.main.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.main.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    WRONG_ACL = '<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>' + config.alt.user_id + '</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>' + config.alt.user_id + '</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>'
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    key.set_xml_acl(CORRECT_ACL)
    e = assert_raises(boto.exception.S3ResponseError, key.set_xml_acl, WRONG_ACL)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets')
@attr(assertion='returns all expected buckets')
def test_buckets_create_then_list():
    create_buckets = [get_new_bucket() for i in xrange(5)]
    list_buckets = s3.main.get_all_buckets()
    names = frozenset(bucket.name for bucket in list_buckets)
    for bucket in create_buckets:
        if bucket.name not in names:
            raise RuntimeError("S3 implementation's GET on Service did not return bucket we created: %r", bucket.name)

# Common code to create a connection object, which'll use bad authorization information
def _create_connection_bad_auth(aws_access_key_id='badauth'):
    # We're going to need to manually build a connection using bad authorization info.
    # But to save the day, lets just hijack the settings from s3.main. :)
    main = s3.main
    conn = boto.s3.connection.S3Connection(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key='roflmao',
        is_secure=main.is_secure,
        port=main.port,
        host=main.host,
        calling_format=main.calling_format,
        )
    return conn

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets (anonymous)')
@attr(assertion='succeeds')
@attr('fails_on_aws')
def test_list_buckets_anonymous():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    conn = _create_connection_bad_auth()
    conn._auth_handler = AnonymousAuth.AnonymousAuthHandler(None, None, None) # Doesn't need this
    buckets = conn.get_all_buckets()
    eq(len(buckets), 0)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets (bad auth)')
@attr(assertion='fails 403')
def test_list_buckets_invalid_auth():
    conn = _create_connection_bad_auth()
    e = assert_raises(boto.exception.S3ResponseError, conn.get_all_buckets)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'InvalidAccessKeyId')

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all buckets (bad auth)')
@attr(assertion='fails 403')
def test_list_buckets_bad_auth():
    conn = _create_connection_bad_auth(aws_access_key_id=s3.main.aws_access_key_id)
    e = assert_raises(boto.exception.S3ResponseError, conn.get_all_buckets)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket')
@attr(assertion='name starts with alphabetic works')
# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
@nose.with_setup(
    setup=lambda: nuke_prefixed_buckets(prefix='a'+get_prefix()),
    teardown=lambda: nuke_prefixed_buckets(prefix='a'+get_prefix()),
    )
def test_bucket_create_naming_good_starts_alpha():
    check_good_bucket_name('foo', _prefix='a'+get_prefix())

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket')
@attr(assertion='name starts with numeric works')
# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
@nose.with_setup(
    setup=lambda: nuke_prefixed_buckets(prefix='0'+get_prefix()),
    teardown=lambda: nuke_prefixed_buckets(prefix='0'+get_prefix()),
    )
def test_bucket_create_naming_good_starts_digit():
    check_good_bucket_name('foo', _prefix='0'+get_prefix())

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket')
@attr(assertion='name containing dot works')
def test_bucket_create_naming_good_contains_period():
    check_good_bucket_name('aaa.111')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket')
@attr(assertion='name containing hyphen works')
def test_bucket_create_naming_good_contains_hyphen():
    check_good_bucket_name('aaa-111')

@attr(resource='bucket')
@attr(method='put')
@attr(operation='create bucket with objects and recreate it')
@attr(assertion='bucket recreation not overriding index')
def test_bucket_recreate_not_overriding():
    key_names = ['mykey1', 'mykey2']
    bucket = _create_keys(keys=key_names)

    li = bucket.list()

    names = [e.name for e in list(li)]
    eq(names, key_names)

    bucket2 = get_new_bucket(targets.main.default, bucket.name)

    li = bucket.list()

    names = [e.name for e in list(li)]
    eq(names, key_names)

@attr(resource='object')
@attr(method='put')
@attr(operation='create and list objects with special names')
@attr(assertion='special names work')
def test_bucket_create_special_key_names():
    key_names = [
        ' ',
        '"',
        '$',
        '%',
        '&',
        '\'',
        '<',
        '>',
        '_',
        '_ ',
        '_ _',
        '__',
    ]
    bucket = _create_keys(keys=key_names)

    li = bucket.list()

    names = [e.name for e in list(li)]
    eq(names, key_names)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='create and list objects with underscore as prefix, list using prefix')
@attr(assertion='listing works correctly')
def test_bucket_list_special_prefix():
    key_names = ['_bla/1', '_bla/2', '_bla/3', '_bla/4', 'abcd']
    bucket = _create_keys(keys=key_names)

    li = bucket.get_all_keys()
    eq(len(li), 5)

    li2 = bucket.get_all_keys(prefix='_bla/')
    eq(len(li2), 4)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy zero sized object in same bucket')
@attr(assertion='works')
def test_object_copy_zero_size():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    fp_a = FakeWriteFile(0, '')
    key.set_contents_from_file(fp_a)
    key.copy(bucket, 'bar321foo')
    key2 = bucket.get_key('bar321foo')
    eq(key2.size, 0)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object in same bucket')
@attr(assertion='works')
def test_object_copy_same_bucket():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    key.copy(bucket, 'bar321foo')
    key2 = bucket.get_key('bar321foo')
    eq(key2.get_contents_as_string(), 'foo')

# http://tracker.ceph.com/issues/11563
@attr(resource='object')
@attr(method='put')
@attr(operation='copy object with content-type')
@attr(assertion='works')
def test_object_copy_verify_contenttype():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    content_type = 'text/bla'
    key.set_contents_from_string('foo',headers={'Content-Type': content_type})
    key.copy(bucket, 'bar321foo')
    key2 = bucket.get_key('bar321foo')
    eq(key2.get_contents_as_string(), 'foo')
    eq(key2.content_type, content_type)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object to itself')
@attr(assertion='fails')
def test_object_copy_to_itself():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    e = assert_raises(boto.exception.S3ResponseError, key.copy, bucket, 'foo123bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidRequest')

@attr(resource='object')
@attr(method='put')
@attr(operation='modify object metadata by copying')
@attr(assertion='fails')
def test_object_copy_to_itself_with_metadata():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    key.copy(bucket, 'foo123bar', {'foo': 'bar'})
    key.close()

    bucket2 = s3.main.get_bucket(bucket.name)
    key2 = bucket2.get_key('foo123bar')
    md = key2.get_metadata('foo')
    eq(md, 'bar')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object from different bucket')
@attr(assertion='works')
def test_object_copy_diff_bucket():
    buckets = [get_new_bucket(), get_new_bucket()]
    key = buckets[0].new_key('foo123bar')
    key.set_contents_from_string('foo')
    key.copy(buckets[1], 'bar321foo')
    key2 = buckets[1].get_key('bar321foo')
    eq(key2.get_contents_as_string(), 'foo')

# is this a necessary check? a NoneType object is being touched here
# it doesn't get to the S3 level
@attr(resource='object')
@attr(method='put')
@attr(operation='copy from an inaccessible bucket')
@attr(assertion='fails w/AttributeError')
def test_object_copy_not_owned_bucket():
    buckets = [get_new_bucket(), get_new_bucket(targets.alt.default)]
    print repr(buckets[1])
    key = buckets[0].new_key('foo123bar')
    key.set_contents_from_string('foo')

    try:
        key.copy(buckets[1], 'bar321foo')
    except AttributeError:
        pass

@attr(resource='object')
@attr(method='put')
@attr(operation='copy a non-owned object in a non-owned bucket, but with perms')
@attr(assertion='works')
def test_object_copy_not_owned_object_bucket():
    bucket = get_new_bucket(targets.main.default)
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')
    bucket.add_user_grant(permission='FULL_CONTROL', user_id=config.alt.user_id, recursive=True)
    k2 = s3.alt.get_bucket(bucket.name).get_key('foo123bar')
    k2.copy(bucket.name, 'bar321foo')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object and change acl')
@attr(assertion='works')
def test_object_copy_canned_acl():
    bucket = get_new_bucket()
    key = bucket.new_key('foo123bar')
    key.set_contents_from_string('foo')

    # use COPY directive
    key2 = bucket.copy_key('bar321foo', bucket.name, 'foo123bar', headers={'x-amz-acl': 'public-read'})
    res = _make_request('GET', bucket, key2)
    eq(res.status, 200)
    eq(res.reason, 'OK')

    # use REPLACE directive
    key3 = bucket.copy_key('bar321foo2', bucket.name, 'foo123bar', headers={'x-amz-acl': 'public-read'}, metadata={'abc': 'def'})
    res = _make_request('GET', bucket, key3)
    eq(res.status, 200)
    eq(res.reason, 'OK')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object and retain metadata')
def test_object_copy_retaining_metadata():
    for size in [3, 1024 * 1024]:
        bucket = get_new_bucket()
        key = bucket.new_key('foo123bar')
        metadata = {'key1': 'value1', 'key2': 'value2'}
        key.set_metadata('key1', 'value1')
        key.set_metadata('key2', 'value2')
        content_type = 'audio/ogg'
        key.content_type = content_type
        key.set_contents_from_string(str(bytearray(size)))

        bucket.copy_key('bar321foo', bucket.name, 'foo123bar')
        key2 = bucket.get_key('bar321foo')
        eq(key2.size, size)
        eq(key2.metadata, metadata)
        eq(key2.content_type, content_type)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy object and replace metadata')
def test_object_copy_replacing_metadata():
    for size in [3, 1024 * 1024]:
        bucket = get_new_bucket()
        key = bucket.new_key('foo123bar')
        key.set_metadata('key1', 'value1')
        key.set_metadata('key2', 'value2')
        key.content_type = 'audio/ogg'
        key.set_contents_from_string(str(bytearray(size)))

        metadata = {'key3': 'value3', 'key1': 'value4'}
        content_type = 'audio/mpeg'
        bucket.copy_key('bar321foo', bucket.name, 'foo123bar', metadata=metadata, headers={'Content-Type': content_type})
        key2 = bucket.get_key('bar321foo')
        eq(key2.size, size)
        eq(key2.metadata, metadata)
        eq(key2.content_type, content_type)

@attr(resource='object')
@attr(method='put')
@attr(operation='copy from non-existent bucket')
def test_object_copy_bucket_not_found():
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3ResponseError, bucket.copy_key, 'foo123bar', bucket.name + "-fake", 'bar321foo')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='put')
@attr(operation='copy from non-existent object')
def test_object_copy_key_not_found():
    bucket = get_new_bucket()
    e = assert_raises(boto.exception.S3ResponseError, bucket.copy_key, 'foo123bar', bucket.name, 'bar321foo')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchKey')

def transfer_part(bucket, mp_id, mp_keyname, i, part):
    """Transfer a part of a multipart upload. Designed to be run in parallel.
    """
    mp = boto.s3.multipart.MultiPartUpload(bucket)
    mp.key_name = mp_keyname
    mp.id = mp_id
    part_out = StringIO(part)
    mp.upload_part_from_file(part_out, i+1)

def copy_part(src_bucket, src_keyname, dst_bucket, dst_keyname, mp_id, i, start=None, end=None):
    """Copy a part of a multipart upload from other bucket.
    """
    mp = boto.s3.multipart.MultiPartUpload(dst_bucket)
    mp.key_name = dst_keyname
    mp.id = mp_id
    mp.copy_part_from_key(src_bucket, src_keyname, i+1, start, end)

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in xrange(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size / chunk):
            s = s + strpart
        if this_part_size > len(s):
            s = s + strpart[0:this_part_size - len(s)]
        yield s
        if (x == size):
            return

def _multipart_upload(bucket, s3_key_name, size, part_size=5*1024*1024, do_list=None, headers=None, metadata=None, resend_parts=[]):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """
    upload = bucket.initiate_multipart_upload(s3_key_name, headers=headers, metadata=metadata)
    s = ''
    for i, part in enumerate(generate_random(size, part_size)):
        s += part
        transfer_part(bucket, upload.id, upload.key_name, i, part)
        if i in resend_parts:
            transfer_part(bucket, upload.id, upload.key_name, i, part)

    if do_list is not None:
        l = bucket.list_multipart_uploads()
        l = list(l)

    return (upload, s)

def _multipart_copy(src_bucketname, src_keyname, dst_bucket, dst_keyname, size, part_size=5*1024*1024, do_list=None, headers=None, metadata=None, resend_parts=[]):
    upload = dst_bucket.initiate_multipart_upload(dst_keyname, headers=headers, metadata=metadata)
    i = 0
    for start_offset in range(0, size, part_size):
        end_offset = min(start_offset + part_size - 1, size - 1)
        copy_part(src_bucketname, src_keyname, dst_bucket, dst_keyname, upload.id, i, start_offset, end_offset)
        if i in resend_parts:
            copy_part(src_bucketname, src_keyname, dst_bucket, dst_name, upload.id, i, start_offset, end_offset)
        i = i + 1

    if do_list is not None:
        l = dst_bucket.list_multipart_uploads()
        l = list(l)

    return upload

def _create_key_with_random_content(keyname, size=7*1024*1024):
    bucket = get_new_bucket()
    key = bucket.new_key(keyname)
    data = StringIO(str(generate_random(size, size).next()))
    key.set_contents_from_file(fp=data)
    return (bucket, key)

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart upload without parts')
def test_multipart_upload_empty():
    bucket = get_new_bucket()
    key = "mymultipart"
    (upload, data) = _multipart_upload(bucket, key, 0)
    e = assert_raises(boto.exception.S3ResponseError, upload.complete_upload)
    eq(e.status, 400)
    eq(e.error_code, u'MalformedXML')

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart uploads with single small part')
def test_multipart_upload_small():
    bucket = get_new_bucket()
    key = "mymultipart"
    size = 1
    (upload, data) = _multipart_upload(bucket, key, size)
    upload.complete_upload()
    key2 = bucket.get_key(key)
    eq(key2.size, size)

def _check_key_content(src, dst):
    assert(src.size >= dst.size)
    src_content = src.get_contents_as_string(headers={'Range': 'bytes={s}-{e}'.format(s=0, e=dst.size-1)})
    dst_content = dst.get_contents_as_string()
    eq(src_content, dst_content)

@attr(resource='object')
@attr(method='put')
@attr(operation='check multipart copies with single small part')
def test_multipart_copy_small():
    (src_bucket, src_key) = _create_key_with_random_content('foo')
    dst_bucket = get_new_bucket()
    dst_keyname = "mymultipart"
    size = 1
    copy = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, size)
    copy.complete_upload()
    key2 = dst_bucket.get_key(dst_keyname)
    eq(key2.size, size)
    _check_key_content(src_key, key2)

def _check_content_using_range(k, data, step):
    objlen = k.size
    for ofs in xrange(0, k.size, step):
        toread = k.size - ofs
        if toread > step:
            toread = step
        end = ofs + toread - 1
        read_range = k.get_contents_as_string(headers={'Range': 'bytes={s}-{e}'.format(s=ofs, e=end)})
        eq(len(read_range), toread)
        eq(read_range, data[ofs:end+1])

@attr(resource='object')
@attr(method='put')
@attr(operation='complete multi-part upload')
@attr(assertion='successful')
def test_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    content_type='text/bla'
    objlen = 30 * 1024 * 1024
    (upload, data) = _multipart_upload(bucket, key, objlen, headers={'Content-Type': content_type}, metadata={'foo': 'bar'})
    upload.complete_upload()

    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 1), 1)
    eq(result.get('x-rgw-bytes-used', 30 * 1024 * 1024), 30 * 1024 * 1024)

    k=bucket.get_key(key)
    eq(k.metadata['foo'], 'bar')
    eq(k.content_type, content_type)
    test_string=k.get_contents_as_string()
    eq(len(test_string), k.size)
    eq(test_string, data)

    _check_content_using_range(k, data, 1000000)
    _check_content_using_range(k, data, 10000000)

def _check_upload_multipart_resend(bucket, key, objlen, resend_parts):
    content_type='text/bla'
    (upload, data) = _multipart_upload(bucket, key, objlen, headers={'Content-Type': content_type}, metadata={'foo': 'bar'}, resend_parts=resend_parts)
    upload.complete_upload()

    k=bucket.get_key(key)
    eq(k.metadata['foo'], 'bar')
    eq(k.content_type, content_type)
    test_string=k.get_contents_as_string()
    eq(k.size, len(test_string))
    eq(k.size, objlen)
    eq(test_string, data)

    _check_content_using_range(k, data, 1000000)
    _check_content_using_range(k, data, 10000000)

@attr(resource='object')
@attr(method='put')
@attr(operation='complete multiple multi-part upload with different sizes')
@attr(resource='object')
@attr(method='put')
@attr(operation='complete multi-part upload')
@attr(assertion='successful')
def test_multipart_upload_resend_part():
    bucket = get_new_bucket()
    key="mymultipart"
    objlen = 30 * 1024 * 1024

    _check_upload_multipart_resend(bucket, key, objlen, [0])
    _check_upload_multipart_resend(bucket, key, objlen, [1])
    _check_upload_multipart_resend(bucket, key, objlen, [2])
    _check_upload_multipart_resend(bucket, key, objlen, [1,2])
    _check_upload_multipart_resend(bucket, key, objlen, [0,1,2,3,4,5])

@attr(assertion='successful')
def test_multipart_upload_multiple_sizes():
    bucket = get_new_bucket()
    key="mymultipart"
    (upload, data) = _multipart_upload(bucket, key, 5 * 1024 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 5 * 1024 * 1024 + 100 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 5 * 1024 * 1024 + 600 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 10 * 1024 * 1024 + 100 * 1024)
    upload.complete_upload()
 
    (upload, data) = _multipart_upload(bucket, key, 10 * 1024 * 1024 + 600 * 1024)
    upload.complete_upload()

    (upload, data) = _multipart_upload(bucket, key, 10 * 1024 * 1024)
    upload.complete_upload()

@attr(assertion='successful')
def test_multipart_copy_multiple_sizes():
    (src_bucket, src_key) = _create_key_with_random_content('foo', 12 * 1024 * 1024)
    dst_bucket = get_new_bucket()
    dst_keyname="mymultipart"

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 5 * 1024 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 5 * 1024 * 1024 + 100 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 5 * 1024 * 1024 + 600 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 10 * 1024 * 1024 + 100 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 10 * 1024 * 1024 + 600 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

    upload = _multipart_copy(src_bucket.name, src_key.name, dst_bucket, dst_keyname, 10 * 1024 * 1024)
    upload.complete_upload()
    _check_key_content(src_key, dst_bucket.get_key(dst_keyname))

@attr(resource='object')
@attr(method='put')
@attr(operation='check failure on multiple multi-part upload with size too small')
@attr(assertion='fails 400')
def test_multipart_upload_size_too_small():
    bucket = get_new_bucket()
    key="mymultipart"
    (upload, data) = _multipart_upload(bucket, key, 100 * 1024, part_size=10*1024)
    e = assert_raises(boto.exception.S3ResponseError, upload.complete_upload)
    eq(e.status, 400)
    eq(e.error_code, u'EntityTooSmall')

def gen_rand_string(size, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def _do_test_multipart_upload_contents(bucket, key_name, num_parts):
    payload=gen_rand_string(5)*1024*1024
    mp=bucket.initiate_multipart_upload(key_name)
    for i in range(0, num_parts):
        mp.upload_part_from_file(StringIO(payload), i+1)

    last_payload='123'*1024*1024
    mp.upload_part_from_file(StringIO(last_payload), num_parts + 1)

    mp.complete_upload()
    key=bucket.get_key(key_name)
    test_string=key.get_contents_as_string()

    all_payload = payload*num_parts + last_payload
    print 'JJJ', key_name, len(all_payload), len(test_string)

    assert test_string == all_payload

    return all_payload


@attr(resource='object')
@attr(method='put')
@attr(operation='check contents of multi-part upload')
@attr(assertion='successful')
def test_multipart_upload_contents():
    _do_test_multipart_upload_contents(get_new_bucket(), 'mymultipart', 3)


@attr(resource='object')
@attr(method='put')
@attr(operation=' multi-part upload overwrites existing key')
@attr(assertion='successful')
def test_multipart_upload_overwrite_existing_object():
    bucket = get_new_bucket()
    key_name="mymultipart"
    payload='12345'*1024*1024
    num_parts=2
    key=bucket.new_key(key_name)
    key.set_contents_from_string(payload)

    mp=bucket.initiate_multipart_upload(key_name)
    for i in range(0, num_parts):
        mp.upload_part_from_file(StringIO(payload), i+1)

    mp.complete_upload()
    key=bucket.get_key(key_name)
    test_string=key.get_contents_as_string()
    assert test_string == payload*num_parts

@attr(resource='object')
@attr(method='put')
@attr(operation='abort multi-part upload')
@attr(assertion='successful')
def test_abort_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    (upload, data) = _multipart_upload(bucket, key, 10 * 1024 * 1024)
    upload.cancel_upload()

    result = _head_bucket(bucket)

    eq(result.get('x-rgw-object-count', 0), 0)
    eq(result.get('x-rgw-bytes-used', 0), 0)

def test_abort_multipart_upload_not_found():
    bucket = get_new_bucket()
    key="mymultipart"
    e = assert_raises(boto.exception.S3ResponseError, bucket.cancel_multipart_upload, key, '1')
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchUpload')

@attr(resource='object')
@attr(method='put')
@attr(operation='concurrent multi-part uploads')
@attr(assertion='successful')
def test_list_multipart_upload():
    bucket = get_new_bucket()
    key="mymultipart"
    mb = 1024 * 1024
    (upload1, data) = _multipart_upload(bucket, key, 5 * mb, do_list = True)
    (upload2, data) = _multipart_upload(bucket, key, 6 * mb, do_list = True)

    key2="mymultipart2"
    (upload3, data) = _multipart_upload(bucket, key2, 5 * mb, do_list = True)

    l = bucket.list_multipart_uploads()
    l = list(l)

    index = dict([(key, 2), (key2, 1)])

    for upload in l:
        index[upload.key_name] -= 1;

    for k, c in index.items():
        eq(c, 0)

    upload1.cancel_upload()
    upload2.cancel_upload()
    upload3.cancel_upload()

@attr(resource='object')
@attr(method='put')
@attr(operation='multi-part upload with missing part')
def test_multipart_upload_missing_part():
    bucket = get_new_bucket()
    key_name = "mymultipart"
    mp = bucket.initiate_multipart_upload(key_name)
    mp.upload_part_from_file(StringIO('\x00'), 1)
    xml = mp.to_xml()
    xml = xml.replace('<PartNumber>1</PartNumber>', '<PartNumber>9999</PartNumber>')
    e = assert_raises(boto.exception.S3ResponseError, bucket.complete_multipart_upload, key_name, mp.id, xml)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidPart')

@attr(resource='object')
@attr(method='put')
@attr(operation='multi-part upload with incorrect ETag')
def test_multipart_upload_incorrect_etag():
    bucket = get_new_bucket()
    key_name = "mymultipart"
    mp = bucket.initiate_multipart_upload(key_name)
    mp.upload_part_from_file(StringIO('\x00'), 1)
    xml = mp.to_xml()
    xml = xml.replace('<ETag>"93b885adfe0da089cdf634904fd59f71"</ETag>', '<ETag>"ffffffffffffffffffffffffffffffff"</ETag>')
    e = assert_raises(boto.exception.S3ResponseError, bucket.complete_multipart_upload, key_name, mp.id, xml)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidPart')

def _simple_http_req_100_cont(host, port, is_secure, method, resource):
    """
    Send the specified request w/expect 100-continue
    and await confirmation.
    """
    req = '{method} {resource} HTTP/1.1\r\nHost: {host}\r\nAccept-Encoding: identity\r\nContent-Length: 123\r\nExpect: 100-continue\r\n\r\n'.format(
            method=method,
            resource=resource,
            host=host,
            )

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if is_secure:
        s = ssl.wrap_socket(s);
    s.settimeout(5)
    s.connect((host, port))
    s.send(req)

    try:
        data = s.recv(1024)
    except socket.error, msg:
        print 'got response: ', msg
        print 'most likely server doesn\'t support 100-continue'

    s.close()
    l = data.split(' ')

    assert l[0].startswith('HTTP')

    return l[1]

@attr(resource='object')
@attr(method='put')
@attr(operation='w/expect continue')
@attr(assertion='succeeds if object is public-read-write')
@attr('100_continue')
@attr('fails_on_mod_proxy_fcgi')
def test_100_continue():
    bucket = get_new_bucket()
    objname = 'testobj'
    resource = '/{bucket}/{obj}'.format(bucket=bucket.name, obj=objname)

    status = _simple_http_req_100_cont(s3.main.host, s3.main.port, s3.main.is_secure, 'PUT', resource)
    eq(status, '403')

    bucket.set_acl('public-read-write')

    status = _simple_http_req_100_cont(s3.main.host, s3.main.port, s3.main.is_secure, 'PUT', resource)
    eq(status, '100')

def _test_bucket_acls_changes_persistent(bucket):
    """
    set and verify readback of each possible permission
    """
    perms = ('FULL_CONTROL', 'WRITE', 'WRITE_ACP', 'READ', 'READ_ACP')
    for p in perms:
        _build_bucket_acl_xml(p, bucket)

@attr(resource='bucket')
@attr(method='put')
@attr(operation='acl set')
@attr(assertion='all permissions are persistent')
def test_bucket_acls_changes_persistent():
    bucket = get_new_bucket()
    _test_bucket_acls_changes_persistent(bucket);

@attr(resource='bucket')
@attr(method='put')
@attr(operation='repeated acl set')
@attr(assertion='all permissions are persistent')
def test_stress_bucket_acls_changes():
    bucket = get_new_bucket()
    for i in xrange(10):
        _test_bucket_acls_changes_persistent(bucket);

@attr(resource='bucket')
@attr(method='put')
@attr(operation='set cors')
@attr(assertion='succeeds')
def test_set_cors():
    bucket = get_new_bucket()
    cfg = CORSConfiguration()
    cfg.add_rule('GET', '*.get')
    cfg.add_rule('PUT', '*.put')

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_cors)
    eq(e.status, 404)

    bucket.set_cors(cfg)
    new_cfg = bucket.get_cors()

    eq(len(new_cfg), 2)

    result = bunch.Bunch()

    for c in new_cfg:
        eq(len(c.allowed_method), 1)
        eq(len(c.allowed_origin), 1)
        result[c.allowed_method[0]] = c.allowed_origin[0]


    eq(result['GET'], '*.get')
    eq(result['PUT'], '*.put')

    bucket.delete_cors()

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_cors)
    eq(e.status, 404)

def _cors_request_and_check(func, url, headers, expect_status, expect_allow_origin, expect_allow_methods):
    r = func(url, headers=headers)
    eq(r.status_code, expect_status)

    assert r.headers['access-control-allow-origin'] == expect_allow_origin
    assert r.headers['access-control-allow-methods'] == expect_allow_methods

    

@attr(resource='bucket')
@attr(method='get')
@attr(operation='check cors response when origin header set')
@attr(assertion='returning cors header')
def test_cors_origin_response():
    cfg = CORSConfiguration()
    bucket = get_new_bucket()

    bucket.set_acl('public-read')

    cfg.add_rule('GET', '*suffix')
    cfg.add_rule('GET', 'start*end')
    cfg.add_rule('GET', 'prefix*')
    cfg.add_rule('PUT', '*.put')

    e = assert_raises(boto.exception.S3ResponseError, bucket.get_cors)
    eq(e.status, 404)

    bucket.set_cors(cfg)

    time.sleep(3) # waiting, since if running against amazon data consistency model is not strict read-after-write

    url = _get_post_url(s3.main, bucket)

    _cors_request_and_check(requests.get, url, None, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.suffix'}, 200, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.bar'}, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.suffix.get'}, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'startend'}, 200, 'startend', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'start1end'}, 200, 'start1end', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'start12end'}, 200, 'start12end', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': '0start12end'}, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'prefix'}, 200, 'prefix', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'prefix.suffix'}, 200, 'prefix.suffix', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'bla.prefix'}, 200, None, None)

    obj_url = '{u}/{o}'.format(u=url, o='bar')
    _cors_request_and_check(requests.get, obj_url, {'Origin': 'foo.suffix'}, 404, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'GET',
                                                    'content-length': '0'}, 403, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'PUT',
                                                    'content-length': '0'}, 403, None, None)
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'DELETE',
                                                    'content-length': '0'}, 403, None, None)
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'content-length': '0'}, 403, None, None)

    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.put', 'content-length': '0'}, 403, 'foo.put', 'PUT')

    _cors_request_and_check(requests.get, obj_url, {'Origin': 'foo.suffix'}, 404, 'foo.suffix', 'GET')

    _cors_request_and_check(requests.options, url, None, 400, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.suffix'}, 400, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'bla'}, 400, None, None)
    _cors_request_and_check(requests.options, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'GET',
                                                    'content-length': '0'}, 200, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.bar', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.suffix.get', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'startend', 'Access-Control-Request-Method': 'GET'}, 200, 'startend', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'start1end', 'Access-Control-Request-Method': 'GET'}, 200, 'start1end', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'start12end', 'Access-Control-Request-Method': 'GET'}, 200, 'start12end', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': '0start12end', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'prefix', 'Access-Control-Request-Method': 'GET'}, 200, 'prefix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'prefix.suffix', 'Access-Control-Request-Method': 'GET'}, 200, 'prefix.suffix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'bla.prefix', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.put', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.put', 'Access-Control-Request-Method': 'PUT'}, 200, 'foo.put', 'PUT')


class FakeFile(object):
    """
    file that simulates seek, tell, and current character
    """
    def __init__(self, char='A', interrupt=None):
        self.offset = 0
        self.char = char
        self.interrupt = interrupt

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_END:
            self.offset = self.size + offset;
        elif whence == os.SEEK_CUR:
            self.offset += offset

    def tell(self):
        return self.offset

class FakeWriteFile(FakeFile):
    """
    file that simulates interruptable reads of constant data
    """
    def __init__(self, size, char='A', interrupt=None):
        FakeFile.__init__(self, char, interrupt)
        self.size = size

    def read(self, size=-1):
        if size < 0:
            size = self.size - self.offset
        count = min(size, self.size - self.offset)
        self.offset += count

        # Sneaky! do stuff before we return (the last time)
        if self.interrupt != None and self.offset == self.size and count > 0:
            self.interrupt()

        return self.char*count

class FakeReadFile(FakeFile):
    """
    file that simulates writes, interrupting after the second
    """
    def __init__(self, size, char='A', interrupt=None):
        FakeFile.__init__(self, char, interrupt)
        self.interrupted = False
        self.size = 0
        self.expected_size = size

    def write(self, chars):
        eq(chars, self.char*len(chars))
        self.offset += len(chars)
        self.size += len(chars)

        # Sneaky! do stuff on the second seek
        if not self.interrupted and self.interrupt != None \
                and self.offset > 0:
            self.interrupt()
            self.interrupted = True

    def close(self):
        eq(self.size, self.expected_size)

class FakeFileVerifier(object):
    """
    file that verifies expected data has been written
    """
    def __init__(self, char=None):
        self.char = char
        self.size = 0

    def write(self, data):
        size = len(data)
        if self.char == None:
            self.char = data[0]
        self.size += size
        eq(data, self.char*size)

def _verify_atomic_key_data(key, size=-1, char=None):
    """
    Make sure file is of the expected size and (simulated) content
    """
    fp_verify = FakeFileVerifier(char)
    key.get_contents_to_file(fp_verify)
    if size >= 0:
        eq(fp_verify.size, size)

def _test_atomic_read(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket = get_new_bucket()
    key = bucket.new_key('testobj')

    # create object of <file_size> As
    fp_a = FakeWriteFile(file_size, 'A')
    key.set_contents_from_file(fp_a)

    read_conn = boto.s3.connection.S3Connection(
        aws_access_key_id=s3['main'].aws_access_key_id,
        aws_secret_access_key=s3['main'].aws_secret_access_key,
        is_secure=s3['main'].is_secure,
        port=s3['main'].port,
        host=s3['main'].host,
        calling_format=s3['main'].calling_format,
        )

    read_bucket = read_conn.get_bucket(bucket.name)
    read_key = read_bucket.get_key('testobj')
    fp_b = FakeWriteFile(file_size, 'B')
    fp_a2 = FakeReadFile(file_size, 'A',
        lambda: key.set_contents_from_file(fp_b)
        )

    # read object while writing it to it
    read_key.get_contents_to_file(fp_a2)
    fp_a2.close()

    _verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='put')
@attr(operation='read atomicity')
@attr(assertion='1MB successful')
def test_atomic_read_1mb():
    _test_atomic_read(1024*1024)

@attr(resource='object')
@attr(method='put')
@attr(operation='read atomicity')
@attr(assertion='4MB successful')
def test_atomic_read_4mb():
    _test_atomic_read(1024*1024*4)

@attr(resource='object')
@attr(method='put')
@attr(operation='read atomicity')
@attr(assertion='8MB successful')
def test_atomic_read_8mb():
    _test_atomic_read(1024*1024*8)

def _test_atomic_write(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Verify the contents are all A's.
    Create a file of B's, use it to re-set_contents_from_file.
    Before re-set continues, verify content's still A's
    Re-read the contents, and confirm we get B's
    """
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    # create <file_size> file of A's
    fp_a = FakeWriteFile(file_size, 'A')
    key.set_contents_from_file(fp_a)

    # verify A's
    _verify_atomic_key_data(key, file_size, 'A')

    read_key = bucket.get_key(objname)

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    fp_b = FakeWriteFile(file_size, 'B',
        lambda: _verify_atomic_key_data(read_key, file_size)
        )
    key.set_contents_from_file(fp_b)

    # verify B's
    _verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='put')
@attr(operation='write atomicity')
@attr(assertion='1MB successful')
def test_atomic_write_1mb():
    _test_atomic_write(1024*1024)

@attr(resource='object')
@attr(method='put')
@attr(operation='write atomicity')
@attr(assertion='4MB successful')
def test_atomic_write_4mb():
    _test_atomic_write(1024*1024*4)

@attr(resource='object')
@attr(method='put')
@attr(operation='write atomicity')
@attr(assertion='8MB successful')
def test_atomic_write_8mb():
    _test_atomic_write(1024*1024*8)

def _test_atomic_dual_write(file_size):
    """
    create an object, two sessions writing different contents
    confirm that it is all one or the other
    """
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    # get a second key object (for the same key)
    # so both can be writing without interfering
    key2 = bucket.new_key(objname)

    # write <file_size> file of B's
    # but before we're done, try to write all A's
    fp_a = FakeWriteFile(file_size, 'A')
    fp_b = FakeWriteFile(file_size, 'B',
        lambda: key2.set_contents_from_file(fp_a, rewind=True)
        )
    key.set_contents_from_file(fp_b)

    # verify the file
    _verify_atomic_key_data(key, file_size)

@attr(resource='object')
@attr(method='put')
@attr(operation='write one or the other')
@attr(assertion='1MB successful')
def test_atomic_dual_write_1mb():
    _test_atomic_dual_write(1024*1024)

@attr(resource='object')
@attr(method='put')
@attr(operation='write one or the other')
@attr(assertion='4MB successful')
def test_atomic_dual_write_4mb():
    _test_atomic_dual_write(1024*1024*4)

@attr(resource='object')
@attr(method='put')
@attr(operation='write one or the other')
@attr(assertion='8MB successful')
def test_atomic_dual_write_8mb():
    _test_atomic_dual_write(1024*1024*8)

def _test_atomic_conditional_write(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Verify the contents are all A's.
    Create a file of B's, use it to re-set_contents_from_file.
    Before re-set continues, verify content's still A's
    Re-read the contents, and confirm we get B's
    """
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    # create <file_size> file of A's
    fp_a = FakeWriteFile(file_size, 'A')
    key.set_contents_from_file(fp_a)

    # verify A's
    _verify_atomic_key_data(key, file_size, 'A')

    read_key = bucket.get_key(objname)

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    fp_b = FakeWriteFile(file_size, 'B',
        lambda: _verify_atomic_key_data(read_key, file_size)
        )
    key.set_contents_from_file(fp_b, headers={'If-Match': '*'})

    # verify B's
    _verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='put')
@attr(operation='write atomicity')
@attr(assertion='1MB successful')
@attr('fails_on_aws')
def test_atomic_conditional_write_1mb():
    _test_atomic_conditional_write(1024*1024)

def _test_atomic_dual_conditional_write(file_size):
    """
    create an object, two sessions writing different contents
    confirm that it is all one or the other
    """
    bucket = get_new_bucket()
    objname = 'testobj'
    key = bucket.new_key(objname)

    fp_a = FakeWriteFile(file_size, 'A')
    key.set_contents_from_file(fp_a)
    _verify_atomic_key_data(key, file_size, 'A')
    etag_fp_a = key.etag.replace('"', '').strip()

    # get a second key object (for the same key)
    # so both can be writing without interfering
    key2 = bucket.new_key(objname)

    # write <file_size> file of C's
    # but before we're done, try to write all B's
    fp_b = FakeWriteFile(file_size, 'B')
    fp_c = FakeWriteFile(file_size, 'C',
        lambda: key2.set_contents_from_file(fp_b, rewind=True, headers={'If-Match': etag_fp_a})
        )
    # key.set_contents_from_file(fp_c, headers={'If-Match': etag_fp_a})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_file, fp_c,
                      headers={'If-Match': etag_fp_a})
    eq(e.status, 412)
    eq(e.reason, 'Precondition Failed')
    eq(e.error_code, 'PreconditionFailed')

    # verify the file
    _verify_atomic_key_data(key, file_size, 'B')

@attr(resource='object')
@attr(method='put')
@attr(operation='write one or the other')
@attr(assertion='1MB successful')
@attr('fails_on_aws')
def test_atomic_dual_conditional_write_1mb():
    _test_atomic_dual_conditional_write(1024*1024)

@attr(resource='object')
@attr(method='put')
@attr(operation='write file in deleted bucket')
@attr(assertion='fail 404')
@attr('fails_on_aws')
def test_atomic_write_bucket_gone():
    bucket = get_new_bucket()

    def remove_bucket():
        bucket.delete()

    # create file of A's but delete the bucket it's in before we finish writing
    # all of them
    key = bucket.new_key('foo')
    fp_a = FakeWriteFile(1024*1024, 'A', remove_bucket)
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_file, fp_a)
    eq(e.status, 404)
    eq(e.reason, 'Not Found')
    eq(e.error_code, 'NoSuchBucket')

@attr(resource='object')
@attr(method='put')
@attr(operation='begin to overwrite file with multipart upload then abort')
@attr(assertion='read back original key contents')
def test_atomic_multipart_upload_write():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')

    upload = bucket.initiate_multipart_upload(key)

    key = bucket.get_key('foo')
    got = key.get_contents_as_string()
    eq(got, 'bar')

    upload.cancel_upload()

    key = bucket.get_key('foo')
    got = key.get_contents_as_string()
    eq(got, 'bar')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns correct data, 206')
def test_ranged_request_response_code():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    key.open('r', headers={'Range': 'bytes=4-7'})
    status = key.resp.status
    content_range = key.resp.getheader('Content-Range')
    fetched_content = ''
    for data in key:
        fetched_content += data;
    key.close()

    eq(fetched_content, content[4:8])
    eq(status, 206)
    eq(content_range, 'bytes 4-7/11')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns correct data, 206')
def test_ranged_request_skip_leading_bytes_response_code():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    # test trailing bytes
    key.open('r', headers={'Range': 'bytes=4-'})
    status = key.resp.status
    content_range = key.resp.getheader('Content-Range')
    fetched_content = ''
    for data in key:
        fetched_content += data;
    key.close()

    eq(fetched_content, content[4:])
    eq(status, 206)
    eq(content_range, 'bytes 4-10/11')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns correct data, 206')
def test_ranged_request_return_trailing_bytes_response_code():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    # test leading bytes
    key.open('r', headers={'Range': 'bytes=-7'})
    status = key.resp.status
    content_range = key.resp.getheader('Content-Range')
    fetched_content = ''
    for data in key:
        fetched_content += data;
    key.close()

    eq(fetched_content, content[-7:])
    eq(status, 206)
    eq(content_range, 'bytes 4-10/11')

@attr(resource='object')
@attr(method='get')
@attr(operation='range')
@attr(assertion='returns invalid range, 416')
def test_ranged_request_invalid_range():
    content = 'testcontent'

    bucket = get_new_bucket()
    key = bucket.new_key('testobj')
    key.set_contents_from_string(content)

    # test invalid range
    e = assert_raises(boto.exception.S3ResponseError, key.open, 'r', headers={'Range': 'bytes=40-50'})
    eq(e.status, 416)
    eq(e.error_code, 'InvalidRange')

def check_can_test_multiregion():
    if not targets.main.master or len(targets.main.secondaries) == 0:
        raise SkipTest

def create_presigned_url(conn, method, bucket_name, key_name, expiration):
    return conn.generate_url(expires_in=expiration,
        method=method,
        bucket=bucket_name,
        key=key_name,
        query_auth=True,
    )

def send_raw_http_request(conn, method, bucket_name, key_name, follow_redirects = False):
    url = create_presigned_url(conn, method, bucket_name, key_name, 3600)
    print url
    h = httplib2.Http()
    h.follow_redirects = follow_redirects
    return h.request(url, method)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='create on one region, access in another')
@attr(assertion='can\'t access in other region')
@attr('multiregion')
def test_region_bucket_create_secondary_access_remove_master():
    check_can_test_multiregion()

    master_conn = targets.main.master.connection

    for r in targets.main.secondaries:
        conn = r.connection
        bucket = get_new_bucket(r)

        r, content = send_raw_http_request(master_conn, 'GET', bucket.name, '', follow_redirects = False)
        eq(r.status, 301)

        r, content = send_raw_http_request(master_conn, 'DELETE', bucket.name, '', follow_redirects = False)
        eq(r.status, 301)

        conn.delete_bucket(bucket)

@attr(resource='bucket')
@attr(method='get')
@attr(operation='create on one region, access in another')
@attr(assertion='can\'t access in other region')
@attr('multiregion')
def test_region_bucket_create_master_access_remove_secondary():
    check_can_test_multiregion()

    master = targets.main.master
    master_conn = master.connection

    for r in targets.main.secondaries:
        conn = r.connection
        bucket = get_new_bucket(master)

        region_sync_meta(targets.main, master)

        r, content = send_raw_http_request(conn, 'GET', bucket.name, '', follow_redirects = False)
        eq(r.status, 301)

        r, content = send_raw_http_request(conn, 'DELETE', bucket.name, '', follow_redirects = False)
        eq(r.status, 301)

        master_conn.delete_bucket(bucket)
        region_sync_meta(targets.main, master)

        e = assert_raises(boto.exception.S3ResponseError, conn.get_bucket, bucket.name)
        eq(e.status, 404)

        e = assert_raises(boto.exception.S3ResponseError, master_conn.get_bucket, bucket.name)
        eq(e.status, 404)


@attr(resource='object')
@attr(method='copy')
@attr(operation='copy object between regions, verify')
@attr(assertion='can read object')
@attr('multiregion')
def test_region_copy_object():
    check_can_test_multiregion()

    for (k, dest) in targets.main.iteritems():
        dest_conn = dest.connection

        dest_bucket = get_new_bucket(dest)
        print 'created new dest bucket ', dest_bucket.name
        region_sync_meta(targets.main, dest)

        if is_slow_backend():
            sizes = (1024, 10 * 1024 * 1024)
        else:
            sizes = (1024, 10 * 1024 * 1024, 100 * 1024 * 1024)

        for file_size in sizes:
            for (k2, r) in targets.main.iteritems():
                if r == dest_conn:
                    continue
                conn = r.connection

                bucket = get_new_bucket(r)
                print 'created bucket', bucket.name
                region_sync_meta(targets.main, r)

                content = 'testcontent'

                print 'creating key=testobj', 'bucket=',bucket.name

                key = bucket.new_key('testobj')
                fp_a = FakeWriteFile(file_size, 'A')
                key.set_contents_from_file(fp_a)

                print 'calling region_sync_meta'

                region_sync_meta(targets.main, r)

                print 'dest_bucket=', dest_bucket.name, 'key=', key.name

                dest_key = dest_bucket.copy_key('testobj-dest', bucket.name, key.name)

                print

                # verify dest
                _verify_atomic_key_data(dest_key, file_size, 'A')

                bucket.delete_key(key.name)

                # confirm that the key was deleted as expected
                region_sync_meta(targets.main, r)
                temp_key = bucket.get_key(key.name)
                assert temp_key == None

                print 'removing bucket', bucket.name
                conn.delete_bucket(bucket)

                # confirm that the bucket was deleted as expected
                region_sync_meta(targets.main, r)
                e = assert_raises(boto.exception.S3ResponseError, conn.get_bucket, bucket.name)
                eq(e.status, 404)
                e = assert_raises(boto.exception.S3ResponseError, dest_conn.get_bucket, bucket.name)
                eq(e.status, 404)

                # confirm that the key was deleted as expected
                dest_bucket.delete_key(dest_key.name)
                temp_key = dest_bucket.get_key(dest_key.name)
                assert temp_key == None


        dest_conn.delete_bucket(dest_bucket)
        region_sync_meta(targets.main, dest)

        # ensure that dest_bucket was deleted on this host and all other hosts
        e = assert_raises(boto.exception.S3ResponseError, dest_conn.get_bucket, dest_bucket.name)
        eq(e.status, 404)
        for (k2, r) in targets.main.iteritems():
            if r == dest_conn:
                continue
            conn = r.connection
            e = assert_raises(boto.exception.S3ResponseError, conn.get_bucket, dest_bucket.name)
            eq(e.status, 404)

def check_versioning(bucket, status):
    try:
        eq(bucket.get_versioning_status()['Versioning'], status)
    except KeyError:
        eq(status, None)

# amazon is eventual consistent, retry a bit if failed
def check_configure_versioning_retry(bucket, status, expected_string):
    bucket.configure_versioning(status)

    read_status = None

    for i in xrange(5):
        try:
            read_status = bucket.get_versioning_status()['Versioning']
        except KeyError:
            read_status = None

        if (expected_string == read_status):
            break

        time.sleep(1)

    eq(expected_string, read_status)


@attr(resource='bucket')
@attr(method='create')
@attr(operation='create versioned bucket')
@attr(assertion='can create and suspend bucket versioning')
@attr('versioning')
def test_versioning_bucket_create_suspend():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, False, "Suspended")
    check_configure_versioning_retry(bucket, True, "Enabled")
    check_configure_versioning_retry(bucket, True, "Enabled")
    check_configure_versioning_retry(bucket, False, "Suspended")


def check_head_obj_content(key, content):
    if content is not None:
        eq(key.get_contents_as_string(), content)
    else:
        print 'check head', key
        eq(key, None)

def check_obj_content(key, content):
    if content is not None:
        eq(key.get_contents_as_string(), content)
    else:
        eq(isinstance(key, boto.s3.deletemarker.DeleteMarker), True)


def check_obj_versions(bucket, objname, keys, contents):
    # check to see if object is pointing at correct version
    key = bucket.get_key(objname)

    if len(contents) > 0:
        print 'testing obj head', objname
        check_head_obj_content(key, contents[-1])
        i = len(contents)
        for key in bucket.list_versions():
            if key.name != objname:
                continue

            i -= 1
            eq(keys[i].version_id or 'null', key.version_id)
            print 'testing obj version-id=', key.version_id
            check_obj_content(key, contents[i])
    else:
        eq(key, None)

def create_multiple_versions(bucket, objname, num_versions, k = None, c = None):
    c = c or []
    k = k or []
    for i in xrange(num_versions):
        c.append('content-{i}'.format(i=i))

        key = bucket.new_key(objname)
        key.set_contents_from_string(c[i])

        if i == 0:
            check_configure_versioning_retry(bucket, True, "Enabled")

    k_pos = len(k)
    i = 0
    for o in bucket.list_versions():
        if o.name != objname:
            continue
        i += 1
        if i > num_versions:
            break

        print o, o.version_id
        k.insert(k_pos, o)
        print 'created obj name=', objname, 'version-id=', o.version_id

    eq(len(k), len(c))

    for j in xrange(num_versions):
        print j, k[j], k[j].version_id

    check_obj_versions(bucket, objname, k, c)

    return (k, c)


def remove_obj_version(bucket, k, c, i):
    # check by versioned key
    i = i % len(k)
    rmkey = k.pop(i)
    content = c.pop(i)
    if (not rmkey.delete_marker):
        eq(rmkey.get_contents_as_string(), content)

    # remove version
    print 'removing version_id=', rmkey.version_id
    bucket.delete_key(rmkey.name, version_id = rmkey.version_id)
    check_obj_versions(bucket, rmkey.name, k, c)

def remove_obj_head(bucket, objname, k, c):
    print 'removing obj=', objname
    key = bucket.delete_key(objname)

    k.append(key)    
    c.append(None)

    eq(key.delete_marker, True)

    check_obj_versions(bucket, objname, k, c)

def _do_test_create_remove_versions(bucket, objname, num_versions, remove_start_idx, idx_inc):
    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    idx = remove_start_idx

    for j in xrange(num_versions):
        remove_obj_version(bucket, k, c, idx)
        idx += idx_inc

def _do_remove_versions(bucket, objname, remove_start_idx, idx_inc, head_rm_ratio, k, c):
    idx = remove_start_idx

    r = 0

    total = len(k)

    for j in xrange(total):
        r += head_rm_ratio
        if r >= 1:
            r %= 1
            remove_obj_head(bucket, objname, k, c)
        else:
            remove_obj_version(bucket, k, c, idx)
            idx += idx_inc

    check_obj_versions(bucket, objname, k, c)

def _do_test_create_remove_versions_and_head(bucket, objname, num_versions, num_ops, remove_start_idx, idx_inc, head_rm_ratio):
    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    _do_remove_versions(bucket, objname, remove_start_idx, idx_inc, head_rm_ratio, k, c)

@attr(resource='object')
@attr(method='create')
@attr(operation='create and remove versioned object')
@attr(assertion='can create access and remove appropriate versions')
@attr('versioning')
def test_versioning_obj_create_read_remove():
    bucket = get_new_bucket()
    objname = 'testobj'
    num_vers = 5

    _do_test_create_remove_versions(bucket, objname, num_vers, -1, 0)
    _do_test_create_remove_versions(bucket, objname, num_vers, -1, 0)
    _do_test_create_remove_versions(bucket, objname, num_vers, 0, 0)
    _do_test_create_remove_versions(bucket, objname, num_vers, 1, 0)
    _do_test_create_remove_versions(bucket, objname, num_vers, 4, -1)
    _do_test_create_remove_versions(bucket, objname, num_vers, 3, 3)

@attr(resource='object')
@attr(method='create')
@attr(operation='create and remove versioned object and head')
@attr(assertion='can create access and remove appropriate versions')
@attr('versioning')
def test_versioning_obj_create_read_remove_head():
    bucket = get_new_bucket()
    objname = 'testobj'
    num_vers = 5

    _do_test_create_remove_versions_and_head(bucket, objname, num_vers, num_vers * 2, -1, 0, 0.5)

def is_null_key(k):
    return (k.version_id is None) or (k.version_id == 'null')

def delete_suspended_versioning_obj(bucket, objname, k, c):
    key = bucket.delete_key(objname)

    i = 0
    while i < len(k):
        if is_null_key(k[i]):
            k.pop(i)
            c.pop(i)
        else:
            i += 1

    key.version_id = "null"
    k.append(key)
    c.append(None)

    check_obj_versions(bucket, objname, k, c)

def overwrite_suspended_versioning_obj(bucket, objname, k, c, content):
    key = bucket.new_key(objname)
    key.set_contents_from_string(content)

    i = 0
    while i < len(k):
        print 'kkk', i, k[i], k[i].version_id
        if is_null_key(k[i]):
            print 'null key!'
            k.pop(i)
            c.pop(i)
        else:
            i += 1

    k.append(key)
    c.append(content)

    check_obj_versions(bucket, objname, k, c)

@attr(resource='object')
@attr(method='create')
@attr(operation='suspend versioned bucket')
@attr(assertion='suspended versioning behaves correctly')
@attr('versioning')
def test_versioning_obj_suspend_versions():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 5
    objname = 'testobj'

    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    check_configure_versioning_retry(bucket, False, "Suspended")

    delete_suspended_versioning_obj(bucket, objname, k, c)
    delete_suspended_versioning_obj(bucket, objname, k, c)
    overwrite_suspended_versioning_obj(bucket, objname, k, c, 'null content 1')
    overwrite_suspended_versioning_obj(bucket, objname, k, c, 'null content 2')
    delete_suspended_versioning_obj(bucket, objname, k, c)
    overwrite_suspended_versioning_obj(bucket, objname, k, c, 'null content 3')
    delete_suspended_versioning_obj(bucket, objname, k, c)

    check_configure_versioning_retry(bucket, True, "Enabled")

    (k, c) = create_multiple_versions(bucket, objname, 3, k, c)

    _do_remove_versions(bucket, objname, 0, 5, 0.5, k, c)
    _do_remove_versions(bucket, objname, 0, 5, 0, k, c)

    eq(len(k), 0)
    eq(len(k), len(c))

@attr(resource='object')
@attr(method='create')
@attr(operation='suspend versioned bucket')
@attr(assertion='suspended versioning behaves correctly')
@attr('versioning')
def test_versioning_obj_suspend_versions_simple():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 1
    objname = 'testobj'

    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    check_configure_versioning_retry(bucket, False, "Suspended")

    delete_suspended_versioning_obj(bucket, objname, k, c)

    check_configure_versioning_retry(bucket, True, "Enabled")

    (k, c) = create_multiple_versions(bucket, objname, 1, k, c)

    for i in xrange(len(k)):
        print 'JJJ: ', k[i].version_id, c[i]

    _do_remove_versions(bucket, objname, 0, 0, 0.5, k, c)
    _do_remove_versions(bucket, objname, 0, 0, 0, k, c)

    eq(len(k), 0)
    eq(len(k), len(c))

@attr(resource='object')
@attr(method='remove')
@attr(operation='create and remove versions')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_obj_create_versions_remove_all():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 10
    objname = 'testobj'

    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    _do_remove_versions(bucket, objname, 0, 5, 0.5, k, c)
    _do_remove_versions(bucket, objname, 0, 5, 0, k, c)

    eq(len(k), 0)
    eq(len(k), len(c))

@attr(resource='object')
@attr(method='remove')
@attr(operation='create and remove versions')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_obj_create_versions_remove_special_names():
    bucket = get_new_bucket()
    check_versioning(bucket, None)

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 10
    objnames = ['_testobj', '_', ':', ' ']

    for objname in objnames:
        (k, c) = create_multiple_versions(bucket, objname, num_versions)

        _do_remove_versions(bucket, objname, 0, 5, 0.5, k, c)
        _do_remove_versions(bucket, objname, 0, 5, 0, k, c)

        eq(len(k), 0)
        eq(len(k), len(c))

@attr(resource='object')
@attr(method='multipart')
@attr(operation='create and test multipart object')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_obj_create_overwrite_multipart():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")

    objname = 'testobj'

    c = []

    num_vers = 3

    for i in xrange(num_vers):
        c.append(_do_test_multipart_upload_contents(bucket, objname, 3))

    k = []
    for key in bucket.list_versions():
        k.insert(0, key)

    eq(len(k), num_vers)
    check_obj_versions(bucket, objname, k, c)

    _do_remove_versions(bucket, objname, 0, 3, 0.5, k, c)
    _do_remove_versions(bucket, objname, 0, 3, 0, k, c)

    eq(len(k), 0)
    eq(len(k), len(c))



@attr(resource='object')
@attr(method='multipart')
@attr(operation='list versioned objects')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_obj_list_marker():
    bucket = get_new_bucket()
    check_configure_versioning_retry(bucket, True, "Enabled")

    objname = 'testobj'
    objname2 = 'testobj-1'

    num_vers = 5

    (k, c) = create_multiple_versions(bucket, objname, num_vers)
    (k2, c2) = create_multiple_versions(bucket, objname2, num_vers)

    k.reverse()
    k2.reverse()

    allkeys = k + k2

    names = []

    for key1, key2 in itertools.izip_longest(bucket.list_versions(), allkeys):
        eq(key1.version_id, key2.version_id)
        names.append(key1.name)

    for i in xrange(len(allkeys)):
        for key1, key2 in itertools.izip_longest(bucket.list_versions(key_marker=names[i], version_id_marker=allkeys[i].version_id), allkeys[i+1:]):
            eq(key1.version_id, key2.version_id)

    # with nonexisting version id, skip to next object
    for key1, key2 in itertools.izip_longest(bucket.list_versions(key_marker=objname, version_id_marker='nosuchversion'), allkeys[5:]):
            eq(key1.version_id, key2.version_id)


@attr(resource='object')
@attr(method='multipart')
@attr(operation='create and test versioned object copying')
@attr(assertion='everything works')
@attr('versioning')
def test_versioning_copy_obj_version():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    num_versions = 3
    objname = 'testobj'

    (k, c) = create_multiple_versions(bucket, objname, num_versions)

    # copy into the same bucket
    for i in xrange(num_versions):
        new_key_name = 'key_{i}'.format(i=i)
        new_key = bucket.copy_key(new_key_name, bucket.name, k[i].name, src_version_id=k[i].version_id)
        eq(new_key.get_contents_as_string(), c[i])

    another_bucket = get_new_bucket()

    # copy into a different bucket
    for i in xrange(num_versions):
        new_key_name = 'key_{i}'.format(i=i)
        new_key = another_bucket.copy_key(new_key_name, bucket.name, k[i].name, src_version_id=k[i].version_id)
        eq(new_key.get_contents_as_string(), c[i])

    # test copy of head object
    new_key = another_bucket.copy_key('new_key', bucket.name, objname)
    eq(new_key.get_contents_as_string(), c[num_versions - 1])

def _count_bucket_versioned_objs(bucket):
    k = []
    for key in bucket.list_versions():
        k.insert(0, key)
    return len(k)


@attr(resource='object')
@attr(method='delete')
@attr(operation='delete multiple versions')
@attr(assertion='deletes multiple versions of an object with a single call')
@attr('versioning')
def test_versioning_multi_object_delete():
	bucket = get_new_bucket()

        check_configure_versioning_retry(bucket, True, "Enabled")

        keyname = 'key'

	key0 = bucket.new_key(keyname)
	key0.set_contents_from_string('foo')
	key1 = bucket.new_key(keyname)
	key1.set_contents_from_string('bar')

        stored_keys = []
        for key in bucket.list_versions():
            stored_keys.insert(0, key)

        eq(len(stored_keys), 2)

	result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 2)
        eq(len(result.errors), 0)

        eq(_count_bucket_versioned_objs(bucket), 0)

        # now remove again, should all succeed due to idempotency
        result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 2)
        eq(len(result.errors), 0)

        eq(_count_bucket_versioned_objs(bucket), 0)

@attr(resource='object')
@attr(method='delete')
@attr(operation='delete multiple versions')
@attr(assertion='deletes multiple versions of an object and delete marker with a single call')
@attr('versioning')
def test_versioning_multi_object_delete_with_marker():
        bucket = get_new_bucket()

        check_configure_versioning_retry(bucket, True, "Enabled")

        keyname = 'key'

	key0 = bucket.new_key(keyname)
	key0.set_contents_from_string('foo')
	key1 = bucket.new_key(keyname)
	key1.set_contents_from_string('bar')

        key2 = bucket.delete_key(keyname)
        eq(key2.delete_marker, True)

        stored_keys = []
        for key in bucket.list_versions():
            stored_keys.insert(0, key)

        eq(len(stored_keys), 3)

	result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 3)
        eq(len(result.errors), 0)
        eq(_count_bucket_versioned_objs(bucket), 0)

        delete_markers = []
        for o in result.deleted:
            if o.delete_marker:
                delete_markers.insert(0, o)

        eq(len(delete_markers), 1)
        eq(key2.version_id, delete_markers[0].version_id)

        # now remove again, should all succeed due to idempotency
        result = bucket.delete_keys(stored_keys)
        eq(len(result.deleted), 3)
        eq(len(result.errors), 0)

        eq(_count_bucket_versioned_objs(bucket), 0)

@attr(resource='object')
@attr(method='delete')
@attr(operation='multi delete create marker')
@attr(assertion='returns correct marker version id')
@attr('versioning')
def test_versioning_multi_object_delete_with_marker_create():
        bucket = get_new_bucket()

        check_configure_versioning_retry(bucket, True, "Enabled")

        keyname = 'key'

        rmkeys = [ bucket.new_key(keyname) ]

        eq(_count_bucket_versioned_objs(bucket), 0)

        result = bucket.delete_keys(rmkeys)
        eq(len(result.deleted), 1)
        eq(_count_bucket_versioned_objs(bucket), 1)

        delete_markers = []
        for o in result.deleted:
            if o.delete_marker:
                delete_markers.insert(0, o)

        eq(len(delete_markers), 1)

        for o in bucket.list_versions():
            eq(o.name, keyname)
            eq(o.version_id, delete_markers[0].delete_marker_version_id)

@attr(resource='object')
@attr(method='put')
@attr(operation='change acl on an object version changes specific version')
@attr(assertion='works')
@attr('versioning')
def test_versioned_object_acl():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    keyname = 'foo'

    key0 = bucket.new_key(keyname)
    key0.set_contents_from_string('bar')
    key1 = bucket.new_key(keyname)
    key1.set_contents_from_string('bla')
    key2 = bucket.new_key(keyname)
    key2.set_contents_from_string('zxc')

    stored_keys = []
    for key in bucket.list_versions():
        stored_keys.insert(0, key)

    k1 = stored_keys[1]

    policy = bucket.get_acl(key_name=k1.name, version_id=k1.version_id)

    default_policy = [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            ]

    print repr(policy)
    check_grants(policy.acl.grants, default_policy)

    bucket.set_canned_acl('public-read', key_name=k1.name, version_id=k1.version_id)

    policy = bucket.get_acl(key_name=k1.name, version_id=k1.version_id)
    print repr(policy)
    check_grants(
        policy.acl.grants,
        [
            dict(
                permission='FULL_CONTROL',
                id=policy.owner.id,
                display_name=policy.owner.display_name,
                uri=None,
                email_address=None,
                type='CanonicalUser',
                ),
            dict(
                permission='READ',
                id=None,
                display_name=None,
                uri='http://acs.amazonaws.com/groups/global/AllUsers',
                email_address=None,
                type='Group',
                ),
            ],
        )

    k = bucket.new_key(keyname)
    check_grants(k.get_acl().acl.grants, default_policy)


def _do_create_object(bucket, objname, i):
    k = bucket.new_key(objname)
    k.set_contents_from_string('data {i}'.format(i=i))

def _do_remove_ver(bucket, obj):
    bucket.delete_key(obj.name, version_id = obj.version_id)

def _do_create_versioned_obj_concurrent(bucket, objname, num):
    t = []
    for i in range(num):
        thr = threading.Thread(target = _do_create_object, args=(bucket, objname, i))
        thr.start()
        t.append(thr)
    return t

def _do_clear_versioned_bucket_concurrent(bucket):
    t = []
    for o in bucket.list_versions():
        thr = threading.Thread(target = _do_remove_ver, args=(bucket, o))
        thr.start()
        t.append(thr)
    return t

def _do_wait_completion(t):
    for thr in t:
        thr.join()

@attr(resource='object')
@attr(method='put')
@attr(operation='concurrent creation of objects, concurrent removal')
@attr(assertion='works')
@attr('versioning')
def test_versioned_concurrent_object_create_concurrent_remove():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    keyname = 'myobj'

    num_objs = 5

    for i in xrange(5):
        t = _do_create_versioned_obj_concurrent(bucket, keyname, num_objs)
        _do_wait_completion(t)

        eq(_count_bucket_versioned_objs(bucket), num_objs)
        eq(len(bucket.get_all_keys()), 1)

        t = _do_clear_versioned_bucket_concurrent(bucket)
        _do_wait_completion(t)

        eq(_count_bucket_versioned_objs(bucket), 0)
        eq(len(bucket.get_all_keys()), 0)

@attr(resource='object')
@attr(method='put')
@attr(operation='concurrent creation and removal of objects')
@attr(assertion='works')
@attr('versioning')
def test_versioned_concurrent_object_create_and_remove():
    bucket = get_new_bucket()

    check_configure_versioning_retry(bucket, True, "Enabled")

    keyname = 'myobj'

    num_objs = 3

    all_threads = []

    for i in xrange(3):
        t = _do_create_versioned_obj_concurrent(bucket, keyname, num_objs)
        all_threads.append(t)

        t = _do_clear_versioned_bucket_concurrent(bucket)
        all_threads.append(t)


    for t in all_threads:
        _do_wait_completion(t)

    t = _do_clear_versioned_bucket_concurrent(bucket)
    _do_wait_completion(t)

    eq(_count_bucket_versioned_objs(bucket), 0)
    eq(len(bucket.get_all_keys()), 0)
