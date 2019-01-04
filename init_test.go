package gofakes3_test

// Initialisation file for tests in the 'gofakes3_test' package. Integration tests
// and the like go in this package as we are unable to use backends without the
// '_test' suffix without causing an import cycle.

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

var (
	logFile string
)

func TestMain(m *testing.M) {
	if err := runTestMain(m); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code, ok := err.(errCode)
		if !ok {
			code = 1
		}
		os.Exit(int(code))
	}
	os.Exit(0)
}

func runTestMain(m *testing.M) error {
	flag.StringVar(&logFile, "fakes3.log", "", "Log file (temp file by default)")
	flag.Parse()

	var logOutput *os.File
	var err error

	if logFile == "" {
		logOutput, err = ioutil.TempFile("", "gofakes3-*.log")
	} else {
		logOutput, err = os.Create(logFile)
	}
	if err != nil {
		return err
	}
	defer logOutput.Close()

	fmt.Fprintf(os.Stderr, "log output redirected to %q\n", logOutput.Name())
	log.SetOutput(logOutput)

	if code := m.Run(); code > 0 {
		return errCode(code)
	}
	return nil
}

type errCode int

func (e errCode) Error() string { return fmt.Sprintf("exit code %d", e) }
