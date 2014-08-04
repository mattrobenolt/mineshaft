package store

import (
	"github.com/mattrobenolt/go-cyanite/aggregate"
	"github.com/mattrobenolt/go-cyanite/metric"
	"github.com/mattrobenolt/go-cyanite/schema"
)

type Storer interface {
	Init()
	SetSchema(*schema.Schema)
	SetAggregation(*aggregate.Aggregation)
	Set(*metric.Point) error
	Close()
}

var store Storer

func Register(s Storer) {
	if store == nil {
		store = s
		store.Init()
	}
}

func Get() Storer {
	return store
}
