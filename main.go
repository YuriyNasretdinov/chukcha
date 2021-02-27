package main

import (
	"log"

	"github.com/YuriyNasretdinov/chukcha/server"
	"github.com/YuriyNasretdinov/chukcha/web"
)

func main() {
	s := web.NewServer(&server.InMemory{})

	log.Printf("Listening connections")
	s.Serve()
}
