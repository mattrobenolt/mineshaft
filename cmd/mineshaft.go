package main

import (
	"github.com/mattrobenolt/mineshaft/aggregate"
	"github.com/mattrobenolt/mineshaft/api"
	"github.com/mattrobenolt/mineshaft/carbon"
	"github.com/mattrobenolt/mineshaft/config"
	"github.com/mattrobenolt/mineshaft/schema"
	"github.com/mattrobenolt/mineshaft/store"

	"log"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	conf := config.Open()
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