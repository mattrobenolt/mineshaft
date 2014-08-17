package api

import (
	"github.com/mattrobenolt/mineshaft/index"
	"github.com/mattrobenolt/mineshaft/store"

	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
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
	// Always kindly end in a newline
	w.Write([]byte{'\n'})
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
		collected = make([]*index.Path, 0)
		ch        = make(chan []*index.Path)
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

func Metrics(w http.ResponseWriter, req *http.Request) {
	log.Println("api:", req)
	var (
		err      error
		to, from int
		q        = req.URL.Query()
		targets  = q["target"]
	)

	if len(targets) == 0 {
		invalidRequest(w)
		return
	}

	if from, err = strconv.Atoi(q.Get("from")); err != nil {
		invalidRequest(w)
		return
	}

	if to, err = strconv.Atoi(q.Get("to")); err != nil {
		invalidRequest(w)
		return
	}

	if from > to {
		invalidRequest(w)
		return
	}

	var wg sync.WaitGroup
	series := make(map[string]map[string]interface{})
	defer func() {
		for _, v := range series {
			v["series"].(store.NullFloat64s).Release()
		}
	}()
	for _, t := range targets {
		wg.Add(1)
		go func(t string) {
			r, data := appStore.Get(t, from, to)
			series[t] = map[string]interface{}{
				"from":   r.Lower,
				"to":     r.Upper,
				"step":   r.Rollup,
				"series": data,
			}
			wg.Done()
		}(t)
	}
	wg.Wait()
	jsonResponse(w, series, http.StatusOK)
}

func Intervals(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("target")
	buckets := appStore.GetBuckets(target)
	jsonResponse(w, buckets, http.StatusOK)
}

var appStore *store.Store

func ListenAndServe(addr string, s *store.Store) error {
	appStore = s
	log.Println("api/api: listening on", addr)

	http.HandleFunc("/ping", Ping)
	http.HandleFunc("/metrics", Metrics)
	http.HandleFunc("/paths", Paths)
	http.HandleFunc("/children", Children)
	http.HandleFunc("/intervals", Intervals)
	panic(http.ListenAndServe(addr, nil))
}
