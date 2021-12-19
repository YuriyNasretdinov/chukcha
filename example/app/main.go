package main

import (
	"log"
	"net/http"

	"github.com/YuriyNasretdinov/chukcha/client"
)

type app struct {
	cl *client.Simple
}

func main() {
	a := &app{
		cl: client.NewSimple([]string{"http://127.0.0.1:8080", "http://127.0.0.1:8081/"}),
	}

	http.HandleFunc("/hello", a.helloHandler)
	http.HandleFunc("/", a.indexHandler)

	go CollectEventsThread(a.cl, "http://localhost:8123/", "/tmp/chukcha-client-state.txt")

	log.Printf("Running")
	log.Fatal(http.ListenAndServe(":80", nil))
}
