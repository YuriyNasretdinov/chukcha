package main

import (
	"io"
	"log"
	"net/http"
)

func (a *app) indexHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", `text/html; charset="UTF-8"`)

	err := a.sendEvent(req.Context(), req)
	if err != nil {
		log.Printf("Failed to send to Chukcha: %v", err)
	}

	io.WriteString(w, `
	<form action="/hello" method="POST">
	<div>Enter your name, comrade: <input type="text" name="name" /></div>
	<input type="submit" value="Greet me" />
	</form>
	`)
}
