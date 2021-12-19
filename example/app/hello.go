package main

import (
	"fmt"
	"html"
	"log"
	"net/http"
)

func (a *app) helloHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", `text/html; charset="UTF-8"`)
	req.ParseForm()

	err := a.sendEvent(req.Context(), req)
	if err != nil {
		log.Printf("Failed to send to Chukcha: %v", err)
	}

	fmt.Fprintf(w, "Hello comrade %s!", html.EscapeString(req.Form.Get("name")))
}
