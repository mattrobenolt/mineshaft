package store

import (
	"github.com/gocql/gocql"
	"github.com/mattrobenolt/mineshaft/aggregate"
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/mattrobenolt/mineshaft/schema"

	"errors"
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

func (d *CassandraDriver) Close() {
	if d.session != nil {
		d.session.Close()
	}
}

func init() {
	// Register this driver so it can be loaded
	Register("cassandra", &CassandraDriver{})
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
