package index

import (
	set "github.com/deckarep/golang-set"
	elastigo "github.com/mattbaird/elastigo/lib"

	"encoding/json"
	"errors"
	"log"
	"net/url"
	"strings"
)

type ElasticSearchDriver struct {
	conn    *elastigo.Conn
	indexer *elastigo.BulkIndexer
	index   string

	cache set.Set
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
	d.cache = set.NewThreadUnsafeSet()
	d.indexer = d.conn.NewBulkIndexer(10)
	d.indexer.Start()
	return nil
}

func (d *ElasticSearchDriver) Update(path string) error {
	// Already did this, guys
	if d.cache.Contains(path) {
		return nil
	}
	d.cache.Add(path)
	end := len(path)
	depth := strings.Count(path, ".")
	leaf := true
	var p Path
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

func (d *ElasticSearchDriver) GetChildren(path string) ([]Path, error) {
	branch := NewBranch(path)
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"wildcard": map[string]interface{}{
							"path.Key": branch.Key + ".*",
						},
					},
					map[string]interface{}{
						"term": map[string]int{
							"path.Depth": branch.Depth + 1,
						},
					},
				},
			},
		},
	}
	resp, err := d.conn.Search(d.index, "path", nil, query)
	if err != nil {
		log.Println("index/elasticsearch:", err)
		return nil, err
	}
	results := make([]Path, len(resp.Hits.Hits))
	for i, hit := range resp.Hits.Hits {
		json.Unmarshal(*hit.Source, &results[i])
	}
	return results, nil
}

func (d *ElasticSearchDriver) Close() {
	return
}

func (d *ElasticSearchDriver) Ping() error {
	if d.conn == nil || d.indexer == nil {
		return errors.New("elasticsearch: uninitialized")
	}
	_, err := d.conn.DoCommand("HEAD", "/"+d.index, nil, nil)
	return err
}

func init() {
	d := &ElasticSearchDriver{}
	Register("elasticsearch", d)
	Register("es", d)
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
