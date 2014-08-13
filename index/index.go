package index

import (
	"net/url"
	"strings"
	"sync"
)

type Store struct {
	driver Driver
}

func (s *Store) Update(path string) error {
	return s.driver.Update(path)
}

func (s *Store) Ping() error {
	return s.driver.Ping()
}

func (s *Store) GetChildren(path string) ([]*Path, error) {
	return s.driver.GetChildren(path)
}

func (s *Store) Query(path string) ([]*Path, error) {
	return s.driver.Query(path)
}

type Path struct {
	Key   string
	Depth int
	Leaf  bool
}

func (p *Path) Release() {
	pathPool.Put(p)
}

var pathPool = sync.Pool{
	New: func() interface{} { return &Path{} },
}

func NewPath() *Path {
	return pathPool.Get().(*Path)
}

func NewLeaf(path string) *Path {
	p := NewPath()
	p.Key = path
	p.Depth = strings.Count(path, ".")
	p.Leaf = true
	return p
}

func NewBranch(path string) *Path {
	p := NewPath()
	p.Key = path
	p.Depth = strings.Count(path, ".")
	p.Leaf = false
	return p
}

type Driver interface {
	Init(*url.URL) error
	Update(string) error
	GetChildren(string) ([]*Path, error)
	Query(string) ([]*Path, error)
	Ping() error
	Close()
}

func Register(key string, d Driver) {
	registry[key] = d
}

func GetDriver(url *url.URL) Driver {
	d, ok := registry[url.Scheme]
	if !ok {
		panic("index: driver not found")
	}
	err := d.Init(url)
	if err != nil {
		panic(err)
	}
	return d
}

func NewFromConnection(url *url.URL) *Store {
	d := GetDriver(url)
	return &Store{d}
}

var registry = make(map[string]Driver)
