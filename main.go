package main

import (
	"github.com/mattrobenolt/go-cyanite/aggregate"
	"github.com/mattrobenolt/go-cyanite/api"
	"github.com/mattrobenolt/go-cyanite/carbon"
	"github.com/mattrobenolt/go-cyanite/config"
	"github.com/mattrobenolt/go-cyanite/schema"
	"github.com/mattrobenolt/go-cyanite/store"

	"flag"
	"log"
	"runtime"
)

var configPath = flag.String("f", "/etc/cyanite/cyanite.conf", "configuration file")

func init() {
	flag.Parse()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	conf := config.LoadFile(*configPath)
	log.Println(conf)

	s := &store.CassandraStore{
		Cluster:  conf.Store.Cluster,
		Keyspace: conf.Store.Keyspace,
	}
	store.Register(s)
	defer s.Close()

	s.SetSchema(schema.LoadFile(conf.Store.Schema))
	s.SetAggregation(aggregate.LoadFile(conf.Store.Aggregates))

	go carbon.ListenAndServe(conf.Carbon.Host + ":" + conf.Carbon.Port)
	go api.ListenAndServe(conf.Http.Host + ":" + conf.Http.Port)
	select {}
}
