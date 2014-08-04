package aggregate

import (
	"github.com/mattrobenolt/mineshaft/metric"
	"github.com/vaughan0/go-ini"

	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
)

type Rule struct {
	name    string
	pattern *regexp.Regexp
	method  AggregateFunc
}

func (r *Rule) String() string { return r.name }

type Aggregation struct {
	rules       []*Rule
	defaultRule *Rule
}

func (a *Aggregation) AddDefaultRule(method string) {
	a.defaultRule = &Rule{
		method: getMethod(method),
	}
}

func getMethod(method string) AggregateFunc {
	switch method {
	case "min":
		return Min
	case "max":
		return Max
	case "sum":
		return Sum
	case "avg":
	case "average":
		return Avg
	}
	panic(fmt.Sprintf("aggregate: Invalid method %s", method))
}

func (a *Aggregation) AddRule(name, pattern, method string) {
	fmt.Println(name, pattern, method)
	rule := &Rule{
		name:    name,
		pattern: regexp.MustCompile(pattern),
		method:  getMethod(method),
	}
	a.rules = append(a.rules, rule)
}

func (a *Aggregation) Match(path string) *Rule {
	for _, rule := range a.rules {
		if rule.pattern.Match([]byte(path)) {
			return rule
		}
	}
	return a.defaultRule
}

type AggregateFunc func([]*metric.Point) float64

func Max(points []*metric.Point) float64 {
	max := float64(0)
	for _, p := range points {
		if p.Value > max {
			max = p.Value
		}
	}
	return max
}

func Min(points []*metric.Point) float64 {
	min := math.MaxFloat64
	for _, p := range points {
		if p.Value < min {
			min = p.Value
		}
	}
	return min
}

func Sum(points []*metric.Point) float64 {
	total := float64(0)
	for _, p := range points {
		total += p.Value
	}
	return total
}

func Avg(points []*metric.Point) float64 {
	return Sum(points) / float64(len(points))
}

func Load(input io.Reader) *Aggregation {
	file, err := ini.Load(input)
	if err != nil {
		panic(err)
	}
	a := &Aggregation{}
	for k, v := range file {
		if k == "default_average" || k == "default" {
			a.AddDefaultRule(v["aggregationMethod"])
		} else {
			a.AddRule(k, v["pattern"], v["aggregationMethod"])
		}
	}
	return a
}

func LoadFile(path string) *Aggregation {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("aggregate: ", err)
	}
	return Load(file)
}
