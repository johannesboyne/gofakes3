package main

import (
	"flag"
	"log"
	"net"
	"net/http"

	"github.com/johannesboyne/gofakes3"
)

func main() {
	db := flag.String("db", "locals3.db", "Database path / name")
	port := flag.String("port", ":9000", "Port to run the service")
	flag.Parse()

	faker := gofakes3.New(*db)
	listenAndServe(*port, faker.Server())
}

func listenAndServe(addr string, handler http.Handler) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Println("failed to listen:", err)
		return
	}

	log.Println("using port:", listener.Addr().(*net.TCPAddr).Port)
	server := &http.Server{Addr: addr, Handler: handler}
	server.Serve(listener)
}
