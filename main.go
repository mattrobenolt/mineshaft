package main

import (
	"github.com/mattrobenolt/go-cyanite/aggregate"
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
	s.SetSchema(schema.LoadFile(conf.Store.Schema))
	s.SetAggregation(aggregate.LoadFile(conf.Store.Aggregates))

	listen := conf.Carbon.Host + ":" + conf.Carbon.Port
	log.Println("Starting carbon on ", listen)
	carbon.RegisterStore(s)
	go carbon.ListenAndServe(listen)
	select {}
}
