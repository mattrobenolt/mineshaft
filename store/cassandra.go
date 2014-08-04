package store

import (
	"github.com/gocql/gocql"
	"github.com/mattrobenolt/mineshaft/aggregate"
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/schema"

	"log"
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

func (s *CassandraStore) SetSchema(schema *schema.Schema) {
	s.schema = schema
}

func (s *CassandraStore) SetAggregation(agg *aggregate.Aggregation) {
	s.aggregation = agg
}

func (s *CassandraStore) Set(p *metric.Point) error {
	start := time.Now()
	rule := s.schema.Match(p.Path)
	defer func() {
		log.Println(p, rule, time.Now().Sub(start))
	}()
	var wg sync.WaitGroup
	for _, bucket := range rule.Buckets {
		wg.Add(1)
		go func(bucket *schema.Bucket) {
			err := s.session.Query(`UPDATE metric USING TTL ? SET data = data + ? AND rollup = ? AND period = ? AND path = ? AND time = ?`,
				int(bucket.Ttl.Seconds()), []float64{p.Value}, int(bucket.Rollup.Seconds()), bucket.Period, p.Path, bucket.RoundDown(p.Timestamp),
			).Exec()
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
