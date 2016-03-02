========================
 S3 compatibility tests
========================

This is a set of completely unofficial Amazon AWS S3 compatibility
tests, that will hopefully be useful to people implementing software
that exposes an S3-like API.

The tests only cover the REST interface.

TODO: test direct HTTP downloads, like a web browser would do.

The tests use the Boto library, so any e.g. HTTP-level differences
that Boto papers over, the tests will not be able to discover. Raw
HTTP tests may be added later.

The tests use the Nose test framework. To get started, ensure you have
the ``virtualenv`` software installed; e.g. on Debian/Ubuntu::

	sudo apt-get install python-virtualenv

and then run::

	./bootstrap

You will need to create a configuration file with the location of the
service and two different credentials, something like this::

	[DEFAULT]
	## this section is just used as default for all the "s3 *"
        ## sections, you can place these variables also directly there

	## replace with e.g. "localhost" to run against local software
	host = s3.amazonaws.com

	## uncomment the port to use something other than 80
	# port = 8080

	## say "no" to disable TLS
	is_secure = yes

	[fixtures]
	## all the buckets created will start with this prefix;
	## {random} will be filled with random characters to pad
	## the prefix to 30 characters long, and avoid collisions
	bucket prefix = YOURNAMEHERE-{random}-

	[s3 main]
	## the tests assume two accounts are defined, "main" and "alt".

	## user_id is a 64-character hexstring
	user_id = 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

	## display name typically looks more like a unix login, "jdoe" etc
	display_name = youruseridhere

	## replace these with your access keys
	access_key = ABCDEFGHIJKLMNOPQRST
	secret_key = abcdefghijklmnopqrstuvwxyzabcdefghijklmn

	[s3 alt]
	## another user account, used for ACL-related tests
	user_id = 56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234
	display_name = john.doe
	## the "alt" user needs to have email set, too
	email = john.doe@example.com
	access_key = NOPQRSTUVWXYZABCDEFG
	secret_key = nopqrstuvwxyzabcdefghijklmnabcdefghijklm

Once you have that, you can run the tests with::

	S3TEST_CONF=your.conf ./virtualenv/bin/nosetests

You can specify what test(s) to run::

	S3TEST_CONF=your.conf ./virtualenv/bin/nosetests s3tests.functional.test_s3:test_bucket_list_empty

Some tests have attributes set based on their current reliability and
things like AWS not enforcing their spec stricly. You can filter tests
based on their attributes::

	S3TEST_CONF=aws.conf ./virtualenv/bin/nosetests -a '!fails_on_aws'


TODO
====

- We should assume read-after-write consistency, and make the tests
  actually request such a location.

  http://aws.amazon.com/s3/faqs/#What_data_consistency_model_does_Amazon_S3_employ
