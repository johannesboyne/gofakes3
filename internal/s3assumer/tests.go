package main

var tests = []Test{
	&S300001GetVersionAfterVersioningSuspended{},
	&S300002DeleteNonexistentVersion{},
	&S300003DeleteVersionFromNonexistentObject{},
	&S300004ListVersionsWithVersionMarkerButNoKeyMarker{},
	&S300005ListVersionsWithNeverVersionedBucket{},
	&S300006GetBucketLocationNoSuchBucket{},
	&S300007BucketVersioningNoSuchBucket{},
}
