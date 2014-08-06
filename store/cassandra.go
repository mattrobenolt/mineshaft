package store

import (
	"github.com/gocql/gocql"
	"github.com/mattrobenolt/mineshaft/aggregate"
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/schema"

	"errors"
	"log"
	"math"
	"sync"
	"time"
)

type CassandraStore struct {
	Cluster  []string
	Keyspace string

	session     *gocql.Session
	schema      *schema.Schema
	aggregation *aggregate.Aggregation
}

func (s *CassandraStore) Init() {
	var err error
	cluster := gocql.NewCluster(s.Cluster...)
	cluster.Keyspace = s.Keyspace
	s.session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
}

func (s *CassandraStore) writeToBucket(p *metric.Point, agg *aggregate.Rule, b *schema.Bucket) error {
	age := int(b.Ttl.Seconds())
	path := p.Path
	rollup := int(b.Rollup.Seconds())
	time := b.RoundDown(p.Timestamp)
	period := b.Period
	value := p.Value

	switch agg.Method {
	case aggregate.MIN:
		timestamp := math.MaxInt64 - int64(value)
		if timestamp <= 0 {
			return errors.New("store: value too small")
		}
		return s.session.Query(
			MINMAX_UPDATE,
			age, timestamp, value, rollup, period, path, time,
		).Exec()
	case aggregate.MAX:
		timestamp := int64(value)
		if timestamp <= 0 {
			return errors.New("store: value too small")
		}
		return s.session.Query(
			MINMAX_UPDATE,
			age, timestamp, value, rollup, period, path, time,
		).Exec()
	case aggregate.SUM:
		return s.session.Query(
			SUM_UPDATE,
			age, toInt64(value), rollup, period, path, time,
		).Exec()
	case aggregate.AVG:
		return s.session.Query(
			AVG_UPDATE,
			age, toInt64(value), rollup, period, path, time,
		).Exec()
	case aggregate.LAST:
		return s.session.Query(
			LAST_UPDATE,
			age, value, rollup, period, path, time,
		).Exec()
	}
	panic("souldn't get here. ever.")
}

func (s *CassandraStore) Set(p *metric.Point) error {
	start := time.Now()
	buckets := s.schema.Match(p.Path).Buckets
	agg := s.aggregation.Match(p.Path)
	defer func() {
		log.Println(p, buckets, agg, time.Now().Sub(start))
	}()
	var wg sync.WaitGroup
	for _, bucket := range buckets {
		wg.Add(1)
		go func(bucket *schema.Bucket) {
			err := s.writeToBucket(p, agg, bucket)
			if err != nil {
				log.Println(err)
			}
			wg.Done()
		}(bucket)
	}
	wg.Wait()
	return nil
}

func (s *CassandraStore) Close() {
	if s.session != nil {
		s.session.Close()
	}
}

func (s *CassandraStore) SetSchema(schema *schema.Schema) {
	s.schema = schema
}

func (s *CassandraStore) SetAggregation(agg *aggregate.Aggregation) {
	s.aggregation = agg
}

// Used for rounding counters
const PRECISION float64 = 100000
const BOTTOM int64 = math.MinInt64 / 2

func toInt64(i float64) int64 {
	return int64(i * PRECISION)
}

func toFloat64(i int64) float64 {
	return float64(i) / PRECISION
}

const AVG_UPDATE = `
UPDATE avg USING TTL ?
SET data = data + ?, count = count + 1
WHERE rollup = ? AND period = ? AND path = ? AND time = ?
`

const SUM_UPDATE = `
UPDATE sum USING TTL ?
SET data = data + ?
WHERE rollup = ? AND period = ? AND path = ? AND time = ?
`
const LAST_UPDATE = `
UPDATE minmaxlast USING TTL ?
SET data = ?
WHERE rollup = ? AND period = ? AND path = ? AND time = ?
`

const MINMAX_UPDATE = `
UPDATE minmaxlast USING TTL ? AND TIMESTAMP ?
SET data = ?
WHERE rollup = ? AND period = ? AND path = ? AND time = ?
`
