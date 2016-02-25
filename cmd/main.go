package main

import (
	"net/http"

	"github.com/johannesboyne/gofakes3"
)

func main() {
	faker := gofakes3.New("locals3.db")
	http.ListenAndServe(":9000", faker.Server())
}
