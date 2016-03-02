#!/bin/bash
#configuration is based on @andrewgaul's work 
#at https://github.com/andrewgaul/s3proxy/blob/master/src/test/resources/run-s3-tests.sh

set -o errexit
set -o nounset

GOFAKEs3_BIN="go run ${PWD}/cmd/main.go"
GOFAKEs3_PORT="9000"
S3TEST_D="${PWD}/s3test/s3-tests"
export S3TEST_CONF="${PWD}/s3test/s3-tests.conf"

# configure s3-tests
pushd $S3TEST_D
./bootstrap
popd

$GOFAKEs3_BIN &
GOFAKEs3_PID=$!

for i in $(seq 30);
do
    if exec 3<>"/dev/tcp/localhost/${GOFAKEs3_PORT}";
    then 
        exec 3<&-  # Close for read
        exec 3>&-  # Close for write
        break
    fi
    sleep 1
done

# execute s3-tests
pushd $S3TEST_D
./virtualenv/bin/nosetests -a '!fails_on_s3proxy'
EXIT_CODE=$?
popd

# clean up and return s3-tests exit code
kill $GOFAKEs3_PID
exit $EXIT_CODE
