package index

import (
	"net/url"
)

type Store struct {
	driver Driver
}

type Driver interface {
	Init(*url.URL) error
	Update(string) error
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
	d.Init(url)
	return d
}

func NewFromConnection(url *url.URL) *Store {
	d := GetDriver(url)
	return &Store{d}
}

var registry = make(map[string]Driver)
