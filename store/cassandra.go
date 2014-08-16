package store

import (
	"github.com/gocql/gocql"
	"github.com/mattrobenolt/mineshaft/aggregate"
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/schema"

	"errors"
	"log"
	"math"
	"net/url"
	"strings"
)

type CassandraDriver struct {
	session     *gocql.Session
	schema      *schema.Schema
	aggregation *aggregate.Aggregation
}

func (d *CassandraDriver) Init(url *url.URL) (err error) {
	cluster := gocql.NewCluster(strings.Split(url.Host, ",")...)
	cluster.Consistency = gocql.One
	cluster.Keyspace = url.Path[1:]
	d.session, err = cluster.CreateSession()
	return err
}

func (d *CassandraDriver) WriteToBucket(p *metric.Point, agg *aggregate.Rule, b *schema.Bucket) error {
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
		return d.session.Query(
			MINMAX_UPDATE,
			age, timestamp, value, rollup, period, path, time,
		).Exec()
	case aggregate.MAX:
		timestamp := int64(value)
		if timestamp <= 0 {
			return errors.New("store: value too small")
		}
		return d.session.Query(
			MINMAX_UPDATE,
			age, timestamp, value, rollup, period, path, time,
		).Exec()
	case aggregate.SUM:
		return d.session.Query(
			SUM_UPDATE,
			age, toInt64(value), rollup, period, path, time,
		).Exec()
	case aggregate.AVG:
		return d.session.Query(
			AVG_UPDATE,
			age, toInt64(value), rollup, period, path, time,
		).Exec()
	case aggregate.LAST:
		return d.session.Query(
			LAST_UPDATE,
			age, value, rollup, period, path, time,
		).Exec()
	}
	panic("souldn't get here. ever.")
}

func (d *CassandraDriver) Get(path string, r *schema.Range, agg *aggregate.Rule) (series NullFloat64s) {
	var iter *gocql.Iter

	log.Println("num_buckets", r.Len())
	series = make(NullFloat64s, r.Len())

	switch agg.Method {
	case aggregate.MIN, aggregate.MAX, aggregate.LAST:
		var data float64
		var time int64
		iter = d.session.Query(
			MINMAXLAST_SELECT,
			r.Rollup, r.Period, path, r.Lower, r.Upper,
		).Consistency(gocql.One).Iter()
		for iter.Scan(&data, &time) {
			series[r.Index(time)] = NewNullFloat64(data, true)
		}
	case aggregate.SUM:
		var data, time int64
		iter = d.session.Query(
			SUM_SELECT,
			r.Rollup, r.Period, path, r.Lower, r.Upper,
		).Consistency(gocql.One).Iter()
		for iter.Scan(&data, &time) {
			series[r.Index(time)] = NewNullFloat64(toFloat64(data), true)
		}
	case aggregate.AVG:
		log.Println("querying avg")
		var data, count, time int64
		iter = d.session.Query(
			AVG_SELECT,
			r.Rollup, r.Period, path, r.Lower, r.Upper,
		).Consistency(gocql.One).Iter()
		for iter.Scan(&data, &count, &time) {
			series[r.Index(time)] = NewNullFloat64(toFloat64(data)/float64(count), true)
		}
	default:
		panic("lol nope")
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
		return nil
	}
	return
}

func (d *CassandraDriver) Close() {
	if d.session != nil {
		d.session.Close()
	}
}

func (d *CassandraDriver) Ping() error {
	// TODO(mattrobeolt): Not sure if this is even too relevant
	// because the entire cluster would have to be down.
	return nil
}

func init() {
	// Register this driver so it can be loaded
	d := &CassandraDriver{}
	Register("cassandra", d)
	Register("cass", d)
}

// Used for rounding counters
const PRECISION float64 = 100000

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

const AVG_SELECT = `
SELECT data, count, time
FROM avg
WHERE rollup = ? AND period = ? AND path = ? AND time >= ? AND time <= ?
`

const SUM_SELECT = `
SELECT data, time
FROM sum
WHERE rollup = ? AND period = ? AND path = ? AND time >= ? AND time <= ?
`

const MINMAXLAST_SELECT = `
SELECT data, time
FROM minmaxlast
WHERE rollup = ? AND period = ? AND path = ? AND time >= ? AND time <= ?
`
