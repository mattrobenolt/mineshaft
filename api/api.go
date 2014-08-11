package api

import (
	"github.com/mattrobenolt/mineshaft/index"
	"github.com/mattrobenolt/mineshaft/store"

	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func invalidRequest(w http.ResponseWriter) {
	jsonResponse(w, "invalid request", http.StatusBadRequest)
}

func jsonResponse(w http.ResponseWriter, data interface{}, status int) {
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// Simple health check endpoint to determine
// if mineshaft is up and able to talk to services
// it depends on.
func Ping(w http.ResponseWriter, r *http.Request) {
	if appStore == nil || !appStore.Ping() {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"status":%d,"errors":[]}`, http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":%d,"errors":[]}`, http.StatusOK)
	}
}

func Children(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	if query == "" {
		invalidRequest(w)
		return
	}

	resp, err := appStore.GetChildren(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, resp, http.StatusOK)
}

func Paths(w http.ResponseWriter, r *http.Request) {
	log.Println("api:", r)
	if r.URL.Query().Get("query") == "" {
		invalidRequest(w)
		return
	}
	var (
		collected = make([]index.Path, 0)
		ch        = make(chan []index.Path)
		queries   = r.URL.Query()["query"]
		received  = 0
	)
	for _, q := range queries {
		go func(q string) {
			resp, err := appStore.QueryIndex(q)
			if err != nil {
				ch <- nil
				return
			}
			ch <- resp
		}(q)
	}
	for {
		resp := <-ch
		if resp != nil {
			collected = append(collected, resp...)
		}
		received++
		if received == len(queries) {
			break
		}
	}
	jsonResponse(w, collected, http.StatusOK)
}

func Metrics(w http.ResponseWriter, r *http.Request) {
	log.Println("api:", r)
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
	http.HandleFunc("/children", Children)
	panic(http.ListenAndServe(addr, nil))
}
