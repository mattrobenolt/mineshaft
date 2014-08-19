package metric

import (
	"sync"
)

func (m *Point) SetPath(v string) {
	m.Path = &v
}

func (m *Point) SetValue(v float64) {
	m.Value = &v
}

func (m *Point) SetTimestamp(v uint32) {
	m.Timestamp = &v
}

func (m *Point) Release() {
	Release(m)
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
	p.Reset()
	pointPool.Put(p)
}
