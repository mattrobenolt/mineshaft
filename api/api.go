package api

import (
	"github.com/mattrobenolt/go-cyanite/store"

	"log"
	"net/http"
)

func Ping(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{}"))
}

func ListenAndServe(addr string) error {
	if store.Get() == nil {
		panic("api: store not set")
	}
	log.Println("Starting api on", addr)

	http.HandleFunc("/ping/", Ping)
	http.HandleFunc("/ping", Ping)
	panic(http.ListenAndServe(addr, nil))
}
