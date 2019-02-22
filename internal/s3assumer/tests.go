package main

var tests = []Test{
	&S300001GetVersionAfterVersioningSuspended{},
	&S300002DeleteNonexistentVersion{},
	&S300003DeleteVersionFromNonexistentObject{},
}
