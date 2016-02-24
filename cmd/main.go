package main

import (
	"github.com/johannesboyne/gofakes3"
)

func main() {
	faker := gofakes3.New()
	faker.StartServer()
}
