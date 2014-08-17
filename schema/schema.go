package schema

import (
	"github.com/vaughan0/go-ini"

	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Bucket struct {
	Ttl    time.Duration
	Period int
	Rollup time.Duration
}

func (b *Bucket) RoundDown(t uint32) uint32 {
	return (t / uint32(b.Rollup.Seconds())) * uint32(b.Rollup.Seconds())
}

type Rule struct {
	name    string
	pattern *regexp.Regexp
	Buckets []*Bucket
}

func (r *Rule) String() string {
	out := r.name
	out += "@["
	for _, b := range r.Buckets {
		out += " " + b.Rollup.String() + ":" + b.Ttl.String()
	}
	out += " ]"
	return out
}

type Schema struct {
	rules       []*Rule
	defaultRule *Rule
}

func (s *Schema) AddDefaultRule(buckets string) {
	s.defaultRule = &Rule{
		Buckets: parseTimeBuckets(buckets),
	}
}

func (s *Schema) AddRule(name, pattern, buckets string) {
	rule := &Rule{
		name:    name,
		pattern: regexp.MustCompile(pattern),
		Buckets: parseTimeBuckets(buckets),
	}
	s.rules = append(s.rules, rule)
}

func (s *Schema) Match(path string) *Rule {
	for _, rule := range s.rules {
		if rule.pattern.Match([]byte(path)) {
			return rule
		}
	}
	return s.defaultRule
}

func (s *Schema) GetRange(path string, from, to int) *Range {
	buckets := s.Match(path).Buckets
	log.Println(buckets)
	duration := to - from
	var bucket *Bucket
	for _, b := range buckets {
		if int(b.Ttl.Seconds()) > duration {
			bucket = b
			break
		}
	}
	if bucket == nil {
		// Just get the largest bucket
		bucket = buckets[len(buckets)-1]
	}
	log.Println("schema:", bucket)
	rollup := int(bucket.Rollup.Seconds())
	return &Range{
		Start:  uint32(from),
		End:    uint32(to),
		Lower:  roundDown(from, rollup),
		Upper:  roundUp(to, rollup),
		Period: bucket.Period,
		Rollup: int(bucket.Rollup.Seconds()),
	}
}

func roundDown(a, interval int) int {
	return int(float64(a)/float64(interval)) * interval
}

func roundUp(a, interval int) int {
	down := roundDown(a, interval)
	if down < a {
		return down + interval
	}
	return down
}

func Load(input io.Reader) *Schema {
	file, err := ini.Load(input)
	if err != nil {
		panic(err)
	}
	s := &Schema{}
	for k, v := range file {
		if k == "everything_else" || k == "default" {
			s.AddDefaultRule(v["retentions"])
		} else {
			s.AddRule(k, v["pattern"], v["retentions"])
		}
	}
	return s
}

func LoadFile(path string) *Schema {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("schema: ", err)
	}
	return Load(file)
}

var defaultSchema = &Schema{}

func parseTimeBuckets(buckets string) (bs []*Bucket) {
	for _, b := range strings.Split(buckets, ",") {
		pieces := strings.SplitN(b, ":", 2)[0:2]
		rollup, ttl := toTime(pieces[0]), toTime(pieces[1])
		bucket := &Bucket{
			Ttl:    ttl,
			Period: int(ttl / rollup),
			Rollup: rollup,
		}
		bs = append(bs, bucket)
	}
	return
}

func toTime(s string) time.Duration {
	re := regexp.MustCompile(`^(\d+)(s|m|min|h|d|w|y)$`)
	pieces := re.FindAllStringSubmatch(s, -1)[0][1:]
	quantity, _ := strconv.Atoi(pieces[0])
	var unit time.Duration
	switch pieces[1] {
	case "s":
		unit = time.Second
	case "m":
	case "min":
		unit = time.Minute
	case "h":
		unit = time.Hour
	case "d":
		unit = 24 * time.Hour
	case "w":
		unit = 7 * 24 * time.Hour
	case "y":
		unit = 365 * 24 * time.Hour
	}
	return time.Duration(quantity) * unit
}

type Range struct {
	Start, End     uint32
	Lower, Upper   int
	Period, Rollup int
}

func (r *Range) Duration() int {
	return r.Upper - r.Lower
}

// How many Rollup intervals are within a Range
func (r *Range) Len() int {
	return r.Duration() / r.Rollup
}

// The index to a bucket within the range
func (r *Range) Index(n int64) int {
	if int(n) < r.Lower || int(n) > r.Upper {
		return -1
	}
	lower := roundDown(int(n), r.Rollup) - r.Lower
	i := lower / r.Rollup
	return i
}
