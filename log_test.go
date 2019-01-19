package gofakes3

import (
	"bytes"
	"log"
	"testing"
)

func TestStdLog(t *testing.T) {
	var buf bytes.Buffer
	std := log.New(&buf, "", 0)
	l := StdLog(std)

	l.Print(LogErr, "yep1", 1)
	l.Print(LogErr, "yep2", 2)
	if buf.String() != "ERR yep1 1\nERR yep2 2\n" {
		t.Fatal()
	}
}

func TestStdLogLevels(t *testing.T) {
	var buf bytes.Buffer
	std := log.New(&buf, "", 0)
	l := StdLog(std, LogErr)

	l.Print(LogErr, "yep1", 1)
	l.Print(LogWarn, "yep2", 2)
	l.Print(LogInfo, "yep3", 3)
	if buf.String() != "ERR yep1 1\n" {
		t.Fatal()
	}
}

func TestDiscardLog(t *testing.T) {
	d := DiscardLog()

	// if it doesn't panic, that's all we can test!
	d.Print(LogErr, "yep1", 1)
	d.Print(LogWarn, "yep2", 2)
	d.Print(LogInfo, "yep3", 3)
}
