package index

import (
	"errors"
	"fmt"
	"strings"
)

type parser struct {
	input  []byte
	output []segment
	i      int
	seg    int
	end    int
	err    error
	method matchtype
}

type typecode int

const (
	STRING typecode = iota
	ANY
	RANGE
	ANY_ONE
	OR
)

type matchtype int

const (
	EXACT matchtype = iota
	REGEXP
)

type chunk struct {
	code  typecode
	value []byte
}

func (c chunk) String() string {
	switch c.code {
	case STRING:
		return fmt.Sprintf("%s", c.value)
	case ANY:
		return "[^.]+"
	case RANGE:
		return fmt.Sprintf("<%c-%c>", c.value[0], c.value[1])
	case ANY_ONE:
		return "."
	case OR:
		return fmt.Sprintf("(%s)", strings.Replace(string(c.value), ",", "|", -1))
	}
	panic("lol")
}

type Query struct {
	// The overarching type of the query.
	// If it's all an EXACT match, we can optimize
	Method matchtype
	// Each path is a group of chunks that represent the query
	Paths []segment
}

type segment []chunk

func (s segment) String() string {
	chunks := make([]string, len(s))
	for i, c := range s {
		chunks[i] = c.String()
	}
	return strings.Join(chunks, "")
}

func (p *parser) convert() []segment {
	err := p.convertPath()
	if err != nil {
		panic(err)
	}
	return p.output
}

func (p *parser) convertPath() error {
	for {
		p.output = append(p.output, make(segment, 0))
		p.convertAny()
		if p.err != nil {
			return p.err
		}
		if p.finished() {
			return nil
		}
	}
	panic("lol")
}

func (p *parser) convertAny() {
	for {
		c := p.input[p.i]
		switch {
		case c == '[':
			p.convertRange()
		case c == '{':
			p.convertOr()
		case c == '*':
			p.convertStar()
		case c == '?':
			p.convertAnyOne()
		case isAlphaNumeric(c):
			p.convertString()
		case c == '.':
			p.seg++
			p.i++
			return
		default:
			p.err = errors.New("invalid character")
			return
		}
		if p.finished() {
			return
		}
	}
	panic("lol")
}

func isAlphaNumeric(c byte) bool {
	return c == '-' || c == '_' || isDigit(c) || isAlpha(c)
}

func isAlpha(c byte) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func (p *parser) convertString() {
	start := p.i
	for {
		p.i++
		if p.finished() {
			break
		}
		c := p.input[p.i]
		if isAlphaNumeric(c) {
			continue
		} else {
			break
		}
	}
	p.addChunk(chunk{STRING, p.input[start:p.i]})
}

func (p *parser) convertRange() {
	p.method = REGEXP
	if p.i+4 >= p.end {
		p.err = errors.New("bad range")
		return
	}
	p.i++
	if !isDigit(p.input[p.i]) {
		p.err = errors.New("bad range")
		return
	}
	p.i++
	if p.input[p.i] != '-' {
		p.err = errors.New("bad range")
		return
	}
	p.i++
	if !isDigit(p.input[p.i]) || p.input[p.i] <= p.input[p.i-2] {
		p.err = errors.New("bad range")
		return
	}
	p.i++
	if p.input[p.i] != ']' {
		p.err = errors.New("bad range")
		return
	}
	p.i++
	p.addChunk(chunk{RANGE, []byte{p.input[p.i-4], p.input[p.i-2]}})
}

func (p *parser) convertOr() {
	p.method = REGEXP
	start := p.i + 1
	for {
		p.i++
		if p.finished() {
			break
		}
		c := p.input[p.i]
		if c == '}' {
			break
		}

		if c == ',' {
			// next
			continue
		}

		// Only allow digits and letters in an OR
		if isDigit(c) || isAlpha(c) {
			continue
		}

		p.err = errors.New("bad OR")
		return
	}
	end := p.i
	p.i++
	p.addChunk(chunk{OR, p.input[start:end]})
}

func (p *parser) convertStar() {
	p.method = REGEXP
	p.i++
	p.addChunk(chunk{ANY, nil})
}

func (p *parser) convertAnyOne() {
	p.method = REGEXP
	p.i++
	p.addChunk(chunk{ANY_ONE, nil})
}

func (p *parser) addChunk(c chunk) {
	p.output[p.seg] = append(p.output[p.seg], c)
}

func (p *parser) finished() bool {
	return p.i == p.end
}

func StringToQuery(query string) *Query {
	p := &parser{input: []byte(query)}
	p.end = len(p.input)
	paths := p.convert()
	return &Query{
		Method: p.method,
		Paths:  paths,
	}
}
