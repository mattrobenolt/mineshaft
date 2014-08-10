package api

import (
	"github.com/mattrobenolt/mineshaft/store"

	"fmt"
	"log"
	"net/http"
)

func Ping(w http.ResponseWriter, r *http.Request) {
	if !appStore.Ping() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(fmt.Sprintf(`{"status":%d,"errors":[]}`, http.StatusServiceUnavailable)))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"status":%d,"errors":[]}`, http.StatusOK)))
	}
	w.Header().Set("Content-Type", "application/json")
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

var appStore *store.Store

func ListenAndServe(addr string, s *store.Store) error {
	appStore = s
	log.Println("Starting api on", addr)

	http.HandleFunc("/ping/", Ping)
	http.HandleFunc("/ping", Ping)
	http.HandleFunc("/metrics", Metrics)
	http.HandleFunc("/paths", Paths)
	panic(http.ListenAndServe(addr, nil))
}
