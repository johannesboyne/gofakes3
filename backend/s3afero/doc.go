package s3afero

/*
Path handling
-------------

The underlying afero.Fs could operate on '/' or '\' paths. The afero library has
punted on specifying this, which means we have to be prepared for any combination
of cases regardless of the operating system: even taking into account the operating
system's path separator, an implementer of afero.Fs may not handle slashes
correctly either.

This means that there are a lot of mixed-up calls to the 'filepath' package, which
uses the Os separator, and the 'path' package, which uses only the '/' characters
S3 expects when simulating filesystems.

Essentially, everything is '/' delimited until the afero boundary.
*/
