package main

import "reflect"

func testName(test Test) string {
	return reflect.TypeOf(test).Name()
}

type Test interface {
	Run(ctx Context) error
}
