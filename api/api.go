package api

import (
	"github.com/mattrobenolt/go-cyanite/store"

	"fmt"
	"log"
	"net/http"
)

func Ping(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{}"))
}

func Paths(w http.ResponseWriter, r *http.Request) {
	log.Println(r)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `[
		{"leaf": true, "path": "%s"}
	]`, r.URL.Query().Get("query"))
}

func Metrics(w http.ResponseWriter, r *http.Request) {
	log.Println(r)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{
		"from": %s,
		"to": %s,
		"step": 10,
		"series": {"%s": []}
	}`, r.URL.Query().Get("from"), r.URL.Query().Get("to"), r.URL.Query().Get("path"))
}

func ListenAndServe(addr string) error {
	if store.Get() == nil {
		panic("api: store not set")
	}
	log.Println("Starting api on", addr)

	http.HandleFunc("/ping/", Ping)
	http.HandleFunc("/ping", Ping)
	http.HandleFunc("/metrics", Metrics)
	http.HandleFunc("/paths", Paths)
	panic(http.ListenAndServe(addr, nil))
}
