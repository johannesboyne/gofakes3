#!/bin/bash
#configuration is based on @andrewgaul's work 
#at https://github.com/andrewgaul/s3proxy/blob/master/src/test/resources/run-s3-tests.sh

set -o errexit
set -o nounset

GOFAKEs3_BIN="${PWD}/cmd/gofakes3"
GOFAKEs3_PORT="9000"
S3TEST_D="${PWD}/s3test/s3-tests"
export S3TEST_CONF="${PWD}/s3test/s3-tests.conf"

# configure s3-tests
pushd $S3TEST_D
./bootstrap
popd

$GOFAKEs3_BIN &
GOFAKEs3_PID=$!
sleep 3

# execute s3-tests
pushd $S3TEST_D
./virtualenv/bin/nosetests -a '!fails_on_s3proxy'
EXIT_CODE=$?
popd

# clean up and return s3-tests exit code
kill $GOFAKEs3_PID
exit $EXIT_CODE
