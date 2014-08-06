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

// Aggregation methods
type Method int

const (
	MIN Method = iota
	MAX
	SUM
	AVG
	LAST
)

type Rule struct {
	name    string
	pattern *regexp.Regexp
	Method  Method
}

func (r *Rule) String() string { return r.name }

type Aggregation struct {
	rules       []*Rule
	defaultRule *Rule
}

func (a *Aggregation) AddDefaultRule(method string) {
	a.defaultRule = &Rule{
		Method: getMethod(method),
	}
}

func getMethod(method string) Method {
	switch method {
	case "min":
		return MIN
	case "max":
		return MAX
	case "sum":
		return SUM
	case "avg":
	case "average":
		return AVG
	case "last":
		return LAST
	}
	panic(fmt.Sprintf("aggregate: Invalid method %s", method))
}

func (a *Aggregation) AddRule(name, pattern, method string) {
	fmt.Println(name, pattern, method)
	rule := &Rule{
		name:    name,
		pattern: regexp.MustCompile(pattern),
		Method:  getMethod(method),
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
