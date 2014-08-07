package index

import (
	set "github.com/deckarep/golang-set"
	elastigo "github.com/mattbaird/elastigo/lib"

	"log"
	"net/url"
)

type ElasticSearchDriver struct {
	conn  *elastigo.BulkIndexer
	index string

	cache set.Set
}

func (d *ElasticSearchDriver) Init(url *url.URL) (err error) {
	conn := elastigo.NewConn()
	conn.Domain = url.Host
	d.index = url.Path[1:]
	d.cache = set.NewThreadUnsafeSet()
	d.conn = conn.NewBulkIndexer(10)
	d.conn.Start()
	return nil
}

func (d *ElasticSearchDriver) Update(path string) error {
	// Already did this, guys
	if d.cache.Contains(path) {
		return nil
	}
	d.cache.Add(path)
	d.conn.Index(d.index, "path", path, "", nil, Path{path}, false)
	log.Println("Updating index", path)
	return nil
}

func (d *ElasticSearchDriver) Close() {
	return
}

func init() {
	d := &ElasticSearchDriver{}
	Register("elasticsearch", d)
	Register("es", d)
}

type Path struct {
	Path string
}
