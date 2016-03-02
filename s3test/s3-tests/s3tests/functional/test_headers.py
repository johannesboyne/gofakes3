from cStringIO import StringIO
import boto.connection
import boto.exception
import boto.s3.connection
import boto.s3.acl
import boto.utils
import bunch
import nose
import operator
import random
import string
import socket
import ssl
import os

from boto.s3.connection import S3Connection

from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from .utils import assert_raises
import AnonymousAuth

from email.header import decode_header

from . import (
    nuke_prefixed_buckets,
    get_new_bucket,
    s3,
    config,
    get_prefix,
    TargetConnection,
    targets,
    )


_orig_conn = {}
_orig_authorize = None
_custom_headers = {}
_remove_headers = []
boto_type = None


# HeaderS3Connection and _our_authorize are necessary to be able to arbitrarily
# overwrite headers. Depending on the version of boto, one or the other is
# necessary. We later determine in setup what needs to be used.

def _update_headers(headers):
    """ update a set of headers with additions/removals
    """
    global _custom_headers, _remove_headers

    headers.update(_custom_headers)

    for header in _remove_headers:
        try:
            del headers[header]
        except KeyError:
            pass


# Note: We need to update the headers twice. The first time so the
# authentication signing is done correctly. The second time to overwrite any
# headers modified or created in the authentication step.

class HeaderS3Connection(S3Connection):
    """ establish an authenticated connection w/customized headers
    """
    def fill_in_auth(self, http_request, **kwargs):
        _update_headers(http_request.headers)
        S3Connection.fill_in_auth(self, http_request, **kwargs)
        _update_headers(http_request.headers)

        return http_request


def _our_authorize(self, connection, **kwargs):
    """ perform an authentication w/customized headers
    """
    _update_headers(self.headers)
    _orig_authorize(self, connection, **kwargs)
    _update_headers(self.headers)


def setup():
    global boto_type

    # we determine what we need to replace by the existence of particular
    # attributes. boto 2.0rc1 as fill_in_auth for S3Connection, while boto 2.0
    # has authorize for HTTPRequest.
    if hasattr(S3Connection, 'fill_in_auth'):
        global _orig_conn

        boto_type = 'S3Connection'
        for conn in s3:
            _orig_conn[conn] = s3[conn]
            header_conn = HeaderS3Connection(
                aws_access_key_id=s3[conn].aws_access_key_id,
                aws_secret_access_key=s3[conn].aws_secret_access_key,
                is_secure=s3[conn].is_secure,
                port=s3[conn].port,
                host=s3[conn].host,
                calling_format=s3[conn].calling_format
                )

            s3[conn] = header_conn
    elif hasattr(boto.connection.HTTPRequest, 'authorize'):
        global _orig_authorize

        boto_type = 'HTTPRequest'

        _orig_authorize = boto.connection.HTTPRequest.authorize
        boto.connection.HTTPRequest.authorize = _our_authorize
    else:
        raise RuntimeError


def teardown():
    global boto_type

    # replace original functionality depending on the boto version
    if boto_type is 'S3Connection':
        global _orig_conn
        for conn in s3:
            s3[conn] = _orig_conn[conn]
        _orig_conn = {}
    elif boto_type is 'HTTPRequest':
        global _orig_authorize

        boto.connection.HTTPRequest.authorize = _orig_authorize
        _orig_authorize = None
    else:
        raise RuntimeError


def _clear_custom_headers():
    """ Eliminate any header customizations
    """
    global _custom_headers, _remove_headers
    _custom_headers = {}
    _remove_headers = []


def _add_custom_headers(headers=None, remove=None):
    """ Define header customizations (additions, replacements, removals)
    """
    global _custom_headers, _remove_headers
    if not _custom_headers:
        _custom_headers = {}

    if headers is not None:
        _custom_headers.update(headers)
    if remove is not None:
        _remove_headers.extend(remove)


def _setup_bad_object(headers=None, remove=None):
    """ Create a new bucket, add an object w/header customizations
    """
    bucket = get_new_bucket()

    _add_custom_headers(headers=headers, remove=remove)
    return bucket.new_key('foo')

def tag(*tags):
    def wrap(func):
        for tag in tags:
            setattr(func, tag, True)
        return func
    return wrap

#
# common tests
#

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid MD5')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_invalid_short():
    key = _setup_bad_object({'Content-MD5':'YWJyYWNhZGFicmE='})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidDigest')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/mismatched MD5')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_bad():
    key = _setup_bad_object({'Content-MD5':'rL0Y20zC+Fzt72VPzMSk2A=='})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'BadDigest')

@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty MD5')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_empty():
    key = _setup_bad_object({'Content-MD5': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidDigest')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphics in MD5')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_unreadable():
    key = _setup_bad_object({'Content-MD5': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no MD5 header')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_none():
    key = _setup_bad_object(remove=('Content-MD5',))
    key.set_contents_from_string('bar')


# strangely, amazon doesn't report an error with a non-expect 100 also, our
# error comes back as html, and not xml as I normally expect
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/Expect 200')
@attr(assertion='garbage, but S3 succeeds!')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_object_create_bad_expect_mismatch():
    key = _setup_bad_object({'Expect': 200})
    key.set_contents_from_string('bar')


# this is a really long test, and I don't know if it's valid...
# again, accepts this with no troubles
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty expect')
@attr(assertion='succeeds ... should it?')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_expect_empty():
    key = _setup_bad_object({'Expect': ''})
    key.set_contents_from_string('bar')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no expect')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_expect_none():
    key = _setup_bad_object(remove=('Expect',))
    key.set_contents_from_string('bar')


# this is a really long test..
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic expect')
@attr(assertion='garbage, but S3 succeeds!')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_object_create_bad_expect_unreadable():
    key = _setup_bad_object({'Expect': '\x07'})
    key.set_contents_from_string('bar')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty content length')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_empty():
    key = _setup_bad_object({'Content-Length': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, None)


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/negative content length')
@attr(assertion='fails 400')
@attr('fails_on_mod_proxy_fcgi')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contentlength_negative():
    key = _setup_bad_object({'Content-Length': -1})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no content length')
@attr(assertion='fails 411')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contentlength_none():
    key = _setup_bad_object(remove=('Content-Length',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 411)
    eq(e.reason, 'Length Required')
    eq(e.error_code,'MissingContentLength')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic content length')
@attr(assertion='fails 400')
@attr('fails_on_mod_proxy_fcgi')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contentlength_unreadable():
    key = _setup_bad_object({'Content-Length': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, None)


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/content length too long')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_object_create_bad_contentlength_mismatch_above():
    content = 'bar'
    length = len(content) + 1

    key = _setup_bad_object({'Content-Length': length})

    # Disable retries since key.should_retry will discard the response with
    # PleaseRetryException.
    def no_retry(response, chunked_transfer): return False
    key.should_retry = no_retry

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, content)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'RequestTimeout')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/content type text/plain')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contenttype_invalid():
    key = _setup_bad_object({'Content-Type': 'text/plain'})
    key.set_contents_from_string('bar')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty content type')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contenttype_empty():
    key = _setup_bad_object({'Content-Type': ''})
    key.set_contents_from_string('bar')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no content type')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contenttype_none():
    key = _setup_bad_object(remove=('Content-Type',))
    key.set_contents_from_string('bar')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic content type')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_object_create_bad_contenttype_unreadable():
    key = _setup_bad_object({'Content-Type': '\x08'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


# the teardown is really messed up here. check it out
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic authorization')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_object_create_bad_authorization_unreadable():
    key = _setup_bad_object({'Authorization': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty authorization')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_authorization_empty():
    key = _setup_bad_object({'Authorization': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


# the teardown is really messed up here. check it out
@tag('auth_common')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no authorization')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_authorization_none():
    key = _setup_bad_object(remove=('Authorization',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no content length')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_contentlength_none():
    _add_custom_headers(remove=('Content-Length',))
    get_new_bucket()


@tag('auth_common')
@attr(resource='bucket')
@attr(method='acls')
@attr(operation='set w/no content length')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_acl_create_contentlength_none():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('blah')

    _add_custom_headers(remove=('Content-Length',))
    key.set_acl('public-read')


@tag('auth_common')
@attr(resource='bucket')
@attr(method='acls')
@attr(operation='set w/invalid permission')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_put_bad_canned_acl():
    bucket = get_new_bucket()

    _add_custom_headers({'x-amz-acl': 'public-ready'})
    e = assert_raises(boto.exception.S3ResponseError, bucket.set_acl, 'public-read')

    eq(e.status, 400)


# strangely, amazon doesn't report an error with a non-expect 100 also, our
# error comes back as html, and not xml as I normally expect
@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/expect 200')
@attr(assertion='garbage, but S3 succeeds!')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_bucket_create_bad_expect_mismatch():
    _add_custom_headers({'Expect':200})
    bucket = get_new_bucket()


# this is a really long test, and I don't know if it's valid...
# again, accepts this with no troubles
@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/expect empty')
@attr(assertion='garbage, but S3 succeeds!')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_expect_empty():
    _add_custom_headers({'Expect': ''})
    bucket = get_new_bucket()


@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/expect nongraphic')
@attr(assertion='garbage, but S3 succeeds!')
# this is a really long test..
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_bucket_create_bad_expect_unreadable():
    _add_custom_headers({'Expect': '\x07'})
    bucket = get_new_bucket()


def _create_new_connection():
    # We're going to need to manually build a connection using bad authorization info.
    # But to save the day, lets just hijack the settings from s3.main. :)
    main = s3.main
    conn = HeaderS3Connection(
        aws_access_key_id=main.aws_access_key_id,
        aws_secret_access_key=main.aws_secret_access_key,
        is_secure=main.is_secure,
        port=main.port,
        host=main.host,
        calling_format=main.calling_format,
        )
    return TargetConnection(targets.main.default.conf, conn)

@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty content length')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_bucket_create_bad_contentlength_empty():
    conn = _create_new_connection()
    _add_custom_headers({'Content-Length': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, conn)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case


@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/negative content length')
@attr(assertion='fails 400')
@attr('fails_on_mod_proxy_fcgi')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_contentlength_negative():
    _add_custom_headers({'Content-Length': -1})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case


@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no content length')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_contentlength_none():
    _add_custom_headers(remove=('Content-Length',))
    bucket = get_new_bucket()


@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/non-graphic content length')
@attr(assertion='fails 400')
@attr('fails_on_mod_proxy_fcgi')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_contentlength_unreadable():
    _add_custom_headers({'Content-Length': '\x07'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, None)


# the teardown is really messed up here. check it out
@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/non-graphic authorization')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
@attr('fails_on_rgw')
def test_bucket_create_bad_authorization_unreadable():
    _add_custom_headers({'Authorization': '\x07'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty authorization')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_authorization_empty():
    _add_custom_headers({'Authorization': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


# the teardown is really messed up here. check it out
@tag('auth_common')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no authorization')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_authorization_none():
    _add_custom_headers(remove=('Authorization',))
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')

#
# AWS2 specific tests
#

@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid MD5')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_invalid_garbage_aws2():
    key = _setup_bad_object({'Content-MD5':'AWS HAHAHA'})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidDigest')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/content length too short')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contentlength_mismatch_below_aws2():
    content = 'bar'
    length = len(content) - 1
    key = _setup_bad_object({'Content-Length': length})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, content)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'BadDigest')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/incorrect authorization')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_authorization_incorrect_aws2():
    key = _setup_bad_object({'Authorization': 'AWS AKIAIGR7ZNNBHC5BKSUB:FWeDfwojDSdS2Ztmpfeubhd9isU='})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch', 'InvalidAccessKeyId')


@tag('auth_aws2')
@nose.with_setup(teardown=_clear_custom_headers)
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid authorization')
@attr(assertion='fails 400')
def test_object_create_bad_authorization_invalid_aws2():
    key = _setup_bad_object({'Authorization': 'AWS HAHAHA'})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidArgument')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty user agent')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_empty_aws2():
    key = _setup_bad_object({'User-Agent': ''})
    key.set_contents_from_string('bar')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic user agent')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_unreadable_aws2():
    key = _setup_bad_object({'User-Agent': '\x07'})
    key.set_contents_from_string('bar')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no user agent')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_none_aws2():
    key = _setup_bad_object(remove=('User-Agent',))
    key.set_contents_from_string('bar')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_invalid_aws2():
    key = _setup_bad_object({'Date': 'Bad Date'})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_empty_aws2():
    key = _setup_bad_object({'Date': ''})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_unreadable_aws2():
    key = _setup_bad_object({'Date': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_none_aws2():
    key = _setup_bad_object(remove=('Date',))
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date in past')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_before_today_aws2():
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 2010 21:53:04 GMT'})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'RequestTimeTooSkewed')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date in future')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_after_today_aws2():
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 2030 21:53:04 GMT'})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'RequestTimeTooSkewed')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date before epoch')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_before_epoch_aws2():
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 1950 21:53:04 GMT'})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date after 9999')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_after_end_aws2():
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 9999 21:53:04 GMT'})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'RequestTimeTooSkewed')


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/invalid authorization')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_authorization_invalid_aws2():
    _add_custom_headers({'Authorization': 'AWS HAHAHA'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidArgument')


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty user agent')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_ua_empty_aws2():
    _add_custom_headers({'User-Agent': ''})
    bucket = get_new_bucket()


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/non-graphic user agent')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_ua_unreadable_aws2():
    _add_custom_headers({'User-Agent': '\x07'})
    bucket = get_new_bucket()


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no user agent')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_ua_none_aws2():
    _add_custom_headers(remove=('User-Agent',))
    bucket = get_new_bucket()


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/invalid date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_invalid_aws2():
    _add_custom_headers({'Date': 'Bad Date'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_empty_aws2():
    _add_custom_headers({'Date': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/non-graphic date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_unreadable_aws2():
    _add_custom_headers({'Date': '\x07'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_none_aws2():
    _add_custom_headers(remove=('Date',))
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date in past')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_before_today_aws2():
    _add_custom_headers({'Date': 'Tue, 07 Jul 2010 21:53:04 GMT'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'RequestTimeTooSkewed')


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date in future')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_after_today_aws2():
    _add_custom_headers({'Date': 'Tue, 07 Jul 2030 21:53:04 GMT'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'RequestTimeTooSkewed')


@tag('auth_aws2')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date before epoch')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_before_epoch_aws2():
    _add_custom_headers({'Date': 'Tue, 07 Jul 1950 21:53:04 GMT'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'AccessDenied')

#
# AWS4 specific tests
#

def check_aws4_support():
    if 'S3_USE_SIGV4' not in os.environ:
       raise SkipTest

@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid MD5')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_md5_invalid_garbage_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Content-MD5':'AWS4 HAHAHA'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidDigest')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/content length too short')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_contentlength_mismatch_below_aws4():
    check_aws4_support()
    content = 'bar'
    length = len(content) - 1
    key = _setup_bad_object({'Content-Length': length})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, content)
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'XAmzContentSHA256Mismatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/incorrect authorization')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_authorization_incorrect_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Authorization': 'AWS4-HMAC-SHA256 Credential=AKIAIGR7ZNNBHC5BKSUB/20150930/us-east-1/s3/aws4_request,SignedHeaders=host;user-agent,Signature=FWeDfwojDSdS2Ztmpfeubhd9isU='})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch', 'InvalidAccessKeyId')


@tag('auth_aws4')
@nose.with_setup(teardown=_clear_custom_headers)
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid authorization')
@attr(assertion='fails 400')
def test_object_create_bad_authorization_invalid_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Authorization': 'AWS4-HMAC-SHA256 Credential=HAHAHA'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    assert e.error_code in ('AuthorizationHeaderMalformed', 'InvalidArgument')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty user agent')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_empty_aws4():
    check_aws4_support()
    key = _setup_bad_object({'User-Agent': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic user agent')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_unreadable_aws4():
    check_aws4_support()
    key = _setup_bad_object({'User-Agent': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no user agent')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_ua_none_aws4():
    check_aws4_support()
    key = _setup_bad_object(remove=('User-Agent',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid date')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_invalid_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Bad Date'})
    key.set_contents_from_string('bar')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/invalid x-amz-date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_amz_date_invalid_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': 'Bad Date'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty date')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_empty_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': ''})
    key.set_contents_from_string('bar')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/empty x-amz-date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_amz_date_empty_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_unreadable_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/non-graphic x-amz-date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_amz_date_unreadable_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no date')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_none_aws4():
    check_aws4_support()
    key = _setup_bad_object(remove=('Date',))
    key.set_contents_from_string('bar')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/no x-amz-date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_amz_date_none_aws4():
    check_aws4_support()
    key = _setup_bad_object(remove=('X-Amz-Date',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date in past')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_before_today_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 2010 21:53:04 GMT'})
    key.set_contents_from_string('bar')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/x-amz-date in past')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_amz_date_before_today_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '20100707T215304Z'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date in future')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_after_today_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 2030 21:53:04 GMT'})
    key.set_contents_from_string('bar')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/x-amz-date in future')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_amz_date_after_today_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '20300707T215304Z'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date before epoch')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_before_epoch_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 1950 21:53:04 GMT'})
    key.set_contents_from_string('bar')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/x-amz-date before epoch')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_amz_date_before_epoch_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '19500707T215304Z'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/date after 9999')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_date_after_end_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 9999 21:53:04 GMT'})
    key.set_contents_from_string('bar')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create w/x-amz-date after 9999')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_bad_amz_date_after_end_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '99990707T215304Z'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(operation='create with missing signed custom header')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_missing_signed_custom_header_aws4():
    check_aws4_support()
    key = _setup_bad_object({'x-zoo': 'zoo'})
    _add_custom_non_auth_headers(remove=('x-zoo',))
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='object')
@attr(method='put')
@attr(opearation='create with missing signed header')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_object_create_missing_signed_header_aws4():
    check_aws4_support()
    key = _setup_bad_object()
    _add_custom_non_auth_headers(remove=('Content-MD5',))
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/invalid authorization')
@attr(assertion='fails 400')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_authorization_invalid_aws4():
    check_aws4_support()
    _add_custom_headers({'Authorization': 'AWS4 HAHAHA'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 400)
    eq(e.reason.lower(), 'bad request') # some proxies vary the case
    eq(e.error_code, 'InvalidArgument')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty user agent')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_ua_empty_aws4():
    check_aws4_support()
    _add_custom_headers({'User-Agent': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/non-graphic user agent')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_ua_unreadable_aws4():
    check_aws4_support()
    _add_custom_headers({'User-Agent': '\x07'})

    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no user agent')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_ua_none_aws4():
    check_aws4_support()
    _add_custom_headers(remove=('User-Agent',))

    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/invalid date')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_invalid_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': 'Bad Date'})
    get_new_bucket()


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/invalid x-amz-date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_amz_date_invalid_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': 'Bad Date'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty date')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_empty_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': ''})
    get_new_bucket()


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/empty x-amz-date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_amz_date_empty_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/non-graphic date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_unreadable_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': '\x07'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    eq(e.error_code, 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/non-graphic x-amz-date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_amz_date_unreadable_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': '\x07'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no date')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_none_aws4():
    check_aws4_support()
    _add_custom_headers(remove=('Date',))
    get_new_bucket()


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/no x-amz-date')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_amz_date_none_aws4():
    check_aws4_support()
    _add_custom_headers(remove=('X-Amz-Date',))
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date in past')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_before_today_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': 'Tue, 07 Jul 2010 21:53:04 GMT'})
    get_new_bucket()


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/x-amz-date in past')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_amz_date_before_today_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': '20100707T215304Z'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date in future')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_after_today_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': 'Tue, 07 Jul 2030 21:53:04 GMT'})
    get_new_bucket()


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/x-amz-date in future')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_amz_date_after_today_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': '20300707T215304Z'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/date before epoch')
@attr(assertion='succeeds')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_date_before_epoch_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': 'Tue, 07 Jul 1950 21:53:04 GMT'})
    get_new_bucket()


@tag('auth_aws4')
@attr(resource='bucket')
@attr(method='put')
@attr(operation='create w/x-amz-date before epoch')
@attr(assertion='fails 403')
@nose.with_setup(teardown=_clear_custom_headers)
def test_bucket_create_bad_amz_date_before_epoch_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': '19500707T215304Z'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    eq(e.status, 403)
    eq(e.reason, 'Forbidden')
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')
