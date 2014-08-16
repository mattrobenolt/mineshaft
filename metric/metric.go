package metric

import (
	"sync"
)

type Point struct {
	Path      string
	Value     float64
	Timestamp uint32
}

func (p *Point) Release() {
	Release(p)
}

type Points []*Point

func (ps Points) Release() {
	for _, p := range ps {
		if p != nil {
			p.Release()
		}
	}
}

var pointPool = sync.Pool{
	New: func() interface{} {
		var p Point
		return &p
	},
}

func New() *Point {
	return pointPool.Get().(*Point)
}

func Release(p *Point) {
	p.Path = ""
	p.Value = 0
	p.Timestamp = 0
	pointPool.Put(p)
}
