package index

import (
	"net/url"
)

type ElasticSearchDriver struct{}

func (d *ElasticSearchDriver) Init(url *url.URL) (err error) {
	return
}

func (d *ElasticSearchDriver) Update(path string) error {
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
