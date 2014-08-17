package store

import (
	"github.com/mattrobenolt/mineshaft/aggregate"
	"github.com/mattrobenolt/mineshaft/index"
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/schema"

	"encoding/json"
	"log"
	"net/url"
	"sync"
	"time"
)

type Store struct {
	driver      Driver
	schema      *schema.Schema
	aggregation *aggregate.Aggregation
	index       *index.Store
}

func (s *Store) Set(p *metric.Point) error {
	var wg sync.WaitGroup

	buckets := s.GetBuckets(p.Path)
	agg := s.aggregation.Match(p.Path)

	// Log the response time
	start := time.Now()
	defer func() {
		log.Println("store/store:", p, buckets, agg, time.Now().Sub(start))
	}()

	go func() {
		wg.Add(1)
		s.index.Update(p.Path)
		wg.Done()
	}()
	for _, bucket := range buckets {
		wg.Add(1)
		go func(bucket *schema.Bucket) {
			err := s.driver.WriteToBucket(p, agg, bucket)
			if err != nil {
				log.Println("store/store:", p, agg, bucket, err)
			}
			wg.Done()
		}(bucket)
	}

	wg.Wait()
	return nil
}

func (s *Store) GetRange(path string, from, to int) *schema.Range {
	return s.schema.GetRange(path, from, to)
}

func (s *Store) Get(path string, from, to int) (*schema.Range, NullFloat64s) {
	r := s.GetRange(path, from, to)
	agg := s.aggregation.Match(path)
	log.Println("store: range", r, "agg", agg)
	return r, s.driver.Get(path, r, agg)
}

func (s *Store) GetBuckets(path string) []*schema.Bucket {
	return s.schema.Match(path).Buckets
}

func (s *Store) Close() {
	if s.driver != nil {
		s.driver.Close()
	}
}

func (s *Store) Ping() bool {
	if err := s.index.Ping(); err != nil {
		log.Println(err)
		return false
	}
	if err := s.driver.Ping(); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (s *Store) SetDriver(driver Driver) {
	s.driver = driver
}

func (s *Store) SetSchema(schema *schema.Schema) {
	s.schema = schema
}

func (s *Store) SetAggregation(agg *aggregate.Aggregation) {
	s.aggregation = agg
}

func (s *Store) SetIndexer(index *index.Store) {
	s.index = index
}

func (s *Store) GetChildren(path string) ([]*index.Path, error) {
	return s.index.GetChildren(path)
}

func (s *Store) QueryIndex(query string) ([]*index.Path, error) {
	return s.index.Query(query)
}

type Driver interface {
	Init(*url.URL) error
	WriteToBucket(*metric.Point, *aggregate.Rule, *schema.Bucket) error
	Get(string, *schema.Range, *aggregate.Rule) NullFloat64s
	Ping() error
	Close()
}

func Register(key string, d Driver) {
	registry[key] = d
}

func GetDriver(url *url.URL) Driver {
	d, ok := registry[url.Scheme]
	if !ok {
		panic("store: driver not found")
	}
	err := d.Init(url)
	if err != nil {
		panic(err)
	}
	return d
}

func NewFromConnection(url *url.URL) *Store {
	d := GetDriver(url)
	return &Store{driver: d}
}

var registry = make(map[string]Driver)

type NullFloat64 struct {
	Float64 float64
	Valid   bool
}

func (nf *NullFloat64) MarshalJSON() ([]byte, error) {
	if !nf.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(nf.Float64)
}

func (nf *NullFloat64) Release() {
	ReleaseNullFloat64(nf)
}

type NullFloat64s []*NullFloat64

func (nfs NullFloat64s) Release() {
	for _, nf := range nfs {
		if nf != nil {
			nf.Release()
		}
	}
}

var nullFloat64Pool = sync.Pool{
	New: func() interface{} {
		var nf NullFloat64
		return &nf
	},
}

func NewNullFloat64(d float64, valid bool) *NullFloat64 {
	nf := nullFloat64Pool.Get().(*NullFloat64)
	nf.Float64 = d
	nf.Valid = valid
	return nf
}

func ReleaseNullFloat64(nf *NullFloat64) {
	nullFloat64Pool.Put(nf)
}
