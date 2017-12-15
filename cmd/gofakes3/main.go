package main

import (
	"flag"
	"net/http"

	"github.com/johannesboyne/gofakes3"
)

func main() {
	db := flag.String("db", "locals3.db", "Database path / name")
	port := flag.String("port", ":9000", "Port to run the service")
	flag.Parse()

	faker := gofakes3.New(*db)
	http.ListenAndServe(*port, faker.Server())
}
