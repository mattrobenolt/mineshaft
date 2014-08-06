package index

import (
	"net/url"
)

// Don't actually use this in production
// Mostly for testing
type MemoryDriver struct{}

func (d *MemoryDriver) Init(url *url.URL) error {
	return nil
}

func (d *MemoryDriver) Update(path string) error {
	return nil
}

func (d *MemoryDriver) Close() {
	return
}

func init() {
	Register("memory", &MemoryDriver{})
}
