package metric

import (
	"sync"
)

type Point struct {
	Path      string
	Value     float64
	Timestamp uint32
}

type Points []*Point

var statsPool = sync.Pool{
	New: func() interface{} { return &Point{} },
}

func New() *Point {
	return statsPool.Get().(*Point)
}

func Release(p *Point) {
	statsPool.Put(p)
}
