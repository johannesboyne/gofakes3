package gofakes3_test

import (
	"testing"
)

func assertStatus(ts *testServer, t *testing.T, url string, code int, descr string) {
	t.Helper()
	client := httpClient()
	rs, err := client.Head(ts.url(url))
	ts.OK(err)
	if rs.StatusCode != code {
		t.Fatal("expected status", code, "found", rs.StatusCode, "descr", descr)
	}
}

func TestRoutingSlashes(t *testing.T) {
	ts := newTestServer(t, withoutInitialBuckets())
	defer ts.Close()
	ts.backendCreateBucket("test")
	ts.backendPutString("test", "obj", nil, "yep")


	assertStatus(ts, t, "nope", 404, "missing bucket") // sanity check missing URL
	assertStatus(ts, t, "test", 200, "only bucket without slash")
	assertStatus(ts, t, "test/", 200, "only bucket with slash")
	assertStatus(ts, t, "test//", 404, "object \"/\"") // obj '/' does not exist
	assertStatus(ts, t, "test/nope", 404, "missing object")
	assertStatus(ts, t, "test/obj", 200, "existing object")
	assertStatus(ts, t, "test/obj/", 404, "trailing slash in object key")
	assertStatus(ts, t, "test/obj//", 404, "two trailing slashes in object key")
}

func TestRoutingMoreSlashes(t *testing.T) {
	testData := [][]string{
		{".", "singledot", "0"},
		{"./", "dotslash", "0"},
		{"./.", "dotslashdot", "0"},
		{"/.", "slashdot", "0"},
		{"/./", "slashdotslash", "0"},
		{"foo/../bar", "singledoubledot", "0"},
		{"foo/../bar/../baz", "twodoubledots", "0"},
		{"/", "singleslash", "0"},
		{"//", "doubleslash", "0"},
		{"///", "threeslashes", "0"},
		{"////", "fourslashes", "0"},
		{"/////", "fiveslashes", "0"},
		{"/a", "leadingsingleslash", "0"},
		{"//a", "leadingdoubleslash", "0"},
		{"///a", "leadingthreeslashes", "0"},
		{"////a", "leadingfourslashes", "0"},
		{"/////a", "leadingfiveslashes", "0"},
		{"a/", "trailingsingleslash", "0"},
		{"a//", "trailingdoubleslash", "0"},
		{"a///", "trailingthreeslashes", "0"},
		{"a////", "trailingfourslashes", "0"},
		{"a/////", "trailingfiveslashes", "0"},
		{"a/a", "middlesingleslash", "0"},
		{"a//a", "middledoubleslash", "0"},
		{"a///a", "middlethreeslashes", "0"},
		{"a////a", "middlefourslashes", "0"},
		{"a/////a", "middlefiveslashes", "0"},
		{"a/a/a/a", "lotofmiddlesingleslashes", "0"},
		{"a//a//a//a", "lotofmiddledoubleslashes", "0"},
		{"a///a///a///a", "lotofmiddlethreeslashes", "0"},
		{"a////a////a////a", "lotofmiddlefourslashes", "0"},
		{"a/////a/////a/////a", "lotofmiddlefiveslashes", "0"},
	}

	testRoutingMoreSlashes(t, testData)

	// reset and reverse testData
	for i, j := 0, len(testData)-1; i < j; i, j = i+1, j-1 {
		testData[i], testData[j] = testData[j], testData[i];
		testData[i][2] = "0"
		testData[j][2] = "0"
	}
	if len(testData) % 2 != 0 {
		testData[((len(testData)-1) / 2)][2] = "0"
	}

	// re-run test in reverse order
	testRoutingMoreSlashes(t, testData)

}

func testRoutingMoreSlashes(t *testing.T, testData [][]string) {
	ts := newTestServer(t, withoutInitialBuckets())
	defer ts.Close()
	ts.backendCreateBucket("test")

	checkObjects := func() {
		t.Helper()
		for _, obj := range testData {
			if obj[2] == "0" {
				assertStatus(ts, t, obj[0], 404, obj[1])
				continue
			}
			if obj[2] == "1" {
				ts.assertObject("test", obj[0], nil, obj[1])
				continue
			}
			t.Fatal("unexpected test data", obj)
		}
	}

	checkObjects()

	for _, obj := range testData {
		ts.backendPutString("test", obj[0], nil, obj[1])
		obj[2] = "1"
		ts.assertObject("test", obj[0], nil, obj[1])
		checkObjects()
	}
}
