package main

import (
	"reflect"
)

func testName(test Test) string {
	v := reflect.ValueOf(test)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v.Type().Name()
}

type Test interface {
	Run(ctx *Context) error
}
