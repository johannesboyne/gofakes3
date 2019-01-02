package gofakes3_test

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
)

func TestCLILsBuckets(t *testing.T) {
	ts := newTestServer(t, withoutInitialBuckets())
	defer ts.Close()
	cli := testCLI{ts}

	if len(cli.lsBuckets()) != 0 {
		t.Fatal()
	}

	ts.createBucket("foo")
	if !reflect.DeepEqual(cli.lsBuckets().Names(), []string{"foo"}) {
		t.Fatal()
	}

	ts.createBucket("bar")
	if !reflect.DeepEqual(cli.lsBuckets().Names(), []string{"bar", "foo"}) {
		t.Fatal()
	}
}

func TestCLILsFiles(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	cli := testCLI{ts}

	if len(cli.lsFiles(defaultBucket, "")) != 0 {
		t.Fatal()
	}

	ts.putString(defaultBucket, "test-one", nil, "hello")
	cli.assertLsFiles(defaultBucket, "",
		nil, []string{"test-one"})

	ts.putString(defaultBucket, "test-two", nil, "hello")
	cli.assertLsFiles(defaultBucket, "",
		nil, []string{"test-one", "test-two"})

	// only "test-one" and "test-two" should pass the prefix match
	ts.putString(defaultBucket, "no-match", nil, "hello")
	cli.assertLsFiles(defaultBucket, "test-",
		nil, []string{"test-one", "test-two"})

	ts.putString(defaultBucket, "test/yep", nil, "hello")
	cli.assertLsFiles(defaultBucket, "",
		[]string{"test/"}, []string{"no-match", "test-one", "test-two"})

	// "test-one" and "test-two" and the directory "test" should pass the prefix match:
	cli.assertLsFiles(defaultBucket, "test",
		[]string{"test/"}, []string{"test-one", "test-two"})

	// listing with a trailing slash should list the directory contents:
	cli.assertLsFiles(defaultBucket, "test/",
		nil, []string{"yep"})
}

type testCLI struct {
	*testServer
}

func (tc *testCLI) command(method string, args ...string) *exec.Cmd {
	tc.Helper()

	if method != "s3" && method != "s3api" {
		panic("expected 's3' or 's3api'")
	}

	cmdArgs := append([]string{
		"--output", "json",
		method,
		"--endpoint", tc.server.URL,
	}, args...)

	cmd := exec.Command("aws", cmdArgs...)
	cmd.Env = []string{
		"AWS_ACCESS_KEY_ID=key",
		"AWS_SECRET_ACCESS_KEY=secret",
	}
	return cmd
}

func (tc *testCLI) combinedOutput(method string, args ...string) (out []byte) {
	tc.Helper()
	out, err := tc.command(method, args...).CombinedOutput()
	if _, ok := err.(*exec.Error); ok {
		tc.Skip("aws cli not found on $PATH")
	}
	tc.OK(err)
	return out
}

var cliLsDirMatcher = regexp.MustCompile(`^\s*PRE (.*)$`)

func (tc *testCLI) assertLsFiles(bucket string, prefix string, dirs []string, files []string) (items cliLsItems) {
	tc.TT.Helper()
	items = tc.lsFiles(bucket, prefix)
	items.assertContents(tc.TT, dirs, files)
	return items
}

func (tc *testCLI) lsFiles(bucket string, prefix string) (items cliLsItems) {
	tc.Helper()

	prefix = strings.TrimLeft(prefix, "/")
	out := tc.combinedOutput("s3", "ls", fmt.Sprintf("s3://%s/%s", bucket, prefix))

	scn := bufio.NewScanner(bytes.NewReader(out))
	for scn.Scan() {
		cur := scn.Text()
		dir := cliLsDirMatcher.FindStringSubmatch(cur)
		if dir != nil {
			items = append(items, cliLsItem{
				isDir: true,
				name:  dir[1], // first submatch
			})

		} else { // file matching
			var ct cliTime
			var item cliLsItem
			tc.OKAll(fmt.Sscan(scn.Text(), &ct, &item.size, &item.name))
			item.date = time.Time(ct)
			items = append(items, item)
		}
	}

	return items
}

func (tc *testCLI) lsBuckets() (buckets gofakes3.Buckets) {
	tc.Helper()

	out := tc.combinedOutput("s3", "ls")

	scn := bufio.NewScanner(bytes.NewReader(out))
	for scn.Scan() {
		var ct cliTime
		var b gofakes3.BucketInfo
		tc.OKAll(fmt.Sscan(scn.Text(), &ct, &b.Name))
		b.CreationDate = ct.contentTime()
		buckets = append(buckets, b)
	}

	return buckets
}

type cliLsItems []cliLsItem

func (cl cliLsItems) assertContents(tt gofakes3.TT, dirs []string, files []string) {
	tt.Helper()
	cl.assertFiles(tt, files...)
	cl.assertDirs(tt, dirs...)
}

func (cl cliLsItems) assertDirs(tt gofakes3.TT, names ...string) {
	tt.Helper()
	cl.assertItems(tt, true, names...)
}

func (cl cliLsItems) assertFiles(tt gofakes3.TT, names ...string) {
	tt.Helper()
	cl.assertItems(tt, false, names...)
}

func (cl cliLsItems) assertItems(tt gofakes3.TT, isDir bool, names ...string) {
	tt.Helper()
	var found []string
	for _, item := range cl {
		if item.isDir == isDir {
			found = append(found, item.name)
		}
	}
	sort.Strings(found)
	sort.Strings(names)
	if !reflect.DeepEqual(found, names) {
		tt.Fatalf("items:\nexp: %v\ngot: %v", names, found)
	}
}

type cliLsItem struct {
	name  string
	date  time.Time
	size  int
	isDir bool
}

type cliTime time.Time

func (c cliTime) contentTime() gofakes3.ContentTime {
	return gofakes3.NewContentTime(time.Time(c))
}

func (c *cliTime) Scan(state fmt.ScanState, verb rune) error {
	d, err := state.Token(false, nil)
	if err != nil {
		return err
	}
	ds := string(d)

	t, err := state.Token(true, nil)
	if err != nil {
		return err
	}
	ts := string(t)

	// CLI returns time in the machine's timezone:
	tv, err := time.ParseInLocation("2006-01-01 15:04:05", ds+" "+ts, time.Local)
	if err != nil {
		return err
	}

	*c = cliTime(tv.In(time.UTC))

	return nil
}
