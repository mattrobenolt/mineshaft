package index

import (
	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/mattrobenolt/mineshaft/set"

	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

type ElasticSearchDriver struct {
	conn    *elastigo.Conn
	indexer *elastigo.BulkIndexer
	index   string

	cache *set.Set
}

func (d *ElasticSearchDriver) Init(url *url.URL) (err error) {
	conn := elastigo.NewConn()
	conn.Domain = url.Host
	d.index = url.Path[1:]
	if ok, _ := conn.ExistsIndex(d.index, "", nil); !ok {
		log.Println("Creating new index in Elasticsearch...")
		resp, err := conn.DoCommand("PUT", "/"+d.index, nil, schema)
		log.Println(string(resp))
		if err != nil {
			panic(err)
		}
		log.Println("ok.")
	}
	d.conn = conn
	d.indexer = d.conn.NewBulkIndexer(10)
	d.indexer.Start()

	var size int
	if url.Query().Get("cache_size") == "" {
		size = 1000
	} else {
		size, _ = strconv.Atoi(url.Query().Get("cache_size"))
	}
	dir := url.Query().Get("cache_dir")
	if dir == "" {
		log.Println("index/elasticsearch: creating a", size, "key in-memory cache")
		d.cache = set.New(size)
	} else {
		log.Println("index/elasticsearch: creating a", size, "key persistent cache in", dir)
		d.cache, _ = set.NewPersistent(size, path.Join(dir, "/keycache.gz"), 1*time.Minute)
	}
	return nil
}

func (d *ElasticSearchDriver) Update(path string) error {
	if !d.cache.Add(path) {
		// path was already cached
		return nil
	}
	log.Println("index/elasticsearch: new path:", path)
	end := len(path)
	depth := strings.Count(path, ".")
	leaf := true
	p := NewPath()
	defer p.Release()
	for end > -1 {
		path = path[0:end]
		p.Key = path
		p.Depth = depth
		p.Leaf = leaf
		// ignoring errors for now
		d.indexer.Index(d.index, "path", p.Key, "", nil, p, false)
		end = strings.LastIndex(path, ".")
		depth--
		leaf = false
	}
	return nil
}

func (d *ElasticSearchDriver) GetChildren(path string) ([]*Path, error) {
	branch := NewBranch(path)
	defer branch.Release()
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"wildcard": map[string]interface{}{
				"path.Key": branch.Key + ".*",
			},
		},
		"filter": map[string]interface{}{
			"term": map[string]int{
				"path.Depth": branch.Depth + 1,
			},
		},
	}
	js, _ := json.Marshal(query)
	log.Println(string(js))
	resp, err := d.conn.Search(d.index, "path", nil, query)
	if err != nil {
		log.Println("index/elasticsearch:", err)
		return nil, err
	}
	return hitsToPaths(resp.Hits), nil
}

func (d *ElasticSearchDriver) Query(path string) ([]*Path, error) {
	q := StringToQuery(path)
	var where map[string]interface{}
	if q.Method == REGEXP {
		where = map[string]interface{}{
			"regexp": map[string]interface{}{
				"path.Key": map[string]interface{}{
					"value": queryToES(q),
					"flags": "INTERVAL",
				},
			},
		}
	} else {
		where = map[string]interface{}{
			"term": map[string]string{
				"path.Key": path,
			},
		}
	}
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"filtered": map[string]interface{}{
				"query": where,
				"filter": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							map[string]interface{}{
								"term": map[string]bool{
									"path.Leaf": true,
								},
							},
							map[string]interface{}{
								"term": map[string]int{
									"path.Depth": len(q.Paths) - 1,
								},
							},
						},
					},
				},
			},
		},
	}
	js, _ := json.Marshal(query)
	log.Println(string(js))
	resp, err := d.conn.Search(d.index, "path", nil, query)
	if err != nil {
		log.Println("index/elasticsearch:", err)
		return nil, err
	}
	return hitsToPaths(resp.Hits), nil
}

func (d *ElasticSearchDriver) Close() {
	d.cache.Close()
	return
}

func (d *ElasticSearchDriver) Ping() error {
	if d.conn == nil || d.indexer == nil {
		return errors.New("elasticsearch: uninitialized")
	}
	_, err := d.conn.DoCommand("HEAD", "/"+d.index, nil, nil)
	return err
}

func hitsToPaths(hits elastigo.Hits) []*Path {
	paths := make([]*Path, len(hits.Hits))
	for i, hit := range hits.Hits {
		json.Unmarshal(*hit.Source, &paths[i])
	}
	return paths
}

func isRegexp(query string) bool {
	for _, c := range query {
		switch c {
		case '[', '{', '*', '<':
			return true
		}
	}
	return false
}

func init() {
	d := &ElasticSearchDriver{}
	Register("elasticsearch", d)
	Register("es", d)
}

func queryToES(q *Query) string {
	out := ""
	for i, p := range q.Paths {
		if i != 0 {
			out += "\\."
		}
		for _, c := range p {
			switch c.code {
			case STRING:
				out += fmt.Sprintf("%s", c.value)
			case ANY:
				out += "[^.]+"
			case RANGE:
				out += fmt.Sprintf("<%c-%c>", c.value[0], c.value[1])
			case ANY_ONE:
				out += "."
			case OR:
				out += fmt.Sprintf("(%s)", strings.Replace(string(c.value), ",", "|", -1))
			}
		}
	}
	return out
}

var schema = map[string]interface{}{
	"settings": map[string]interface{}{
		"analysis": map[string]interface{}{
			"analyzer": map[string]interface{}{
				"mineshaft-analyzer": map[string]string{
					"type":      "custom",
					"tokenizer": "mineshaft-tokenizer",
				},
			},
			"tokenizer": map[string]interface{}{
				"mineshaft-tokenizer": map[string]string{
					"type":      "path_hierarchy",
					"delimiter": ".",
				},
			},
		},
	},
	"mappings": map[string]interface{}{
		"path": map[string]interface{}{
			"properties": map[string]interface{}{
				"Key": map[string]string{
					"type":            "string",
					"index_analyzer":  "mineshaft-analyzer",
					"search_analyzer": "keyword",
				},
				"Depth": map[string]string{
					"type": "integer",
				},
				"Leaf": map[string]string{
					"type": "boolean",
				},
			},
		},
	},
}
