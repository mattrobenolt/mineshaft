package config

import (
	"github.com/mattrobenolt/mineshaft/aggregate"
	"github.com/mattrobenolt/mineshaft/index"
	log "github.com/mattrobenolt/mineshaft/logging"
	"github.com/mattrobenolt/mineshaft/schema"
	"github.com/mattrobenolt/mineshaft/store"
	"github.com/vaughan0/go-ini"

	"flag"
	"net/url"
	"os"
)

var configPath = flag.String("f", "/etc/mineshaft/mineshaft.conf", "configuration file")

func init() {
	flag.Parse()
}

type Config struct {
	CarbonAscii struct {
		Enabled bool
		Host    string
		Port    string
	}
	CarbonPickle struct {
		Enabled bool
		Host    string
		Port    string
	}
	CarbonProtobuf struct {
		Enabled bool
		Host    string
		Port    string
	}
	Http struct {
		Host string
		Port string
	}
	Store struct {
		Connection *url.URL
		Schema     string
		Aggregates string
	}
	Index struct {
		Connection *url.URL
	}
}

func (c *Config) OpenStore() (*store.Store, error) {
	// TODO(mattrobenolt) better error handling here instead of relying on panics
	s := store.NewFromConnection(c.Store.Connection)
	s.SetIndexer(index.NewFromConnection(c.Index.Connection))
	s.SetSchema(schema.LoadFile(c.Store.Schema))
	s.SetAggregation(aggregate.LoadFile(c.Store.Aggregates))
	return s, nil
}

// Load an return a Config object by path
func LoadFile(path string) (*Config, error) {
	log.Println("loading config", path)
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	file, err := ini.Load(fp)
	if err != nil {
		return nil, err
	}
	var c Config
	if _, ok := file["carbon-ascii"]; ok {
		c.CarbonAscii.Host = file["carbon-ascii"]["host"]
		c.CarbonAscii.Port = file["carbon-ascii"]["port"]
		c.CarbonAscii.Enabled = true
	}
	if _, ok := file["carbon-pickle"]; ok {
		c.CarbonPickle.Host = file["carbon-pickle"]["host"]
		c.CarbonPickle.Port = file["carbon-pickle"]["port"]
		c.CarbonPickle.Enabled = true
	}
	if _, ok := file["carbon-protobuf"]; ok {
		c.CarbonProtobuf.Host = file["carbon-protobuf"]["host"]
		c.CarbonProtobuf.Port = file["carbon-protobuf"]["port"]
		c.CarbonProtobuf.Enabled = true
	}
	c.Http.Host = file["http"]["host"]
	c.Http.Port = file["http"]["port"]
	c.Store.Connection, _ = url.Parse(file["store"]["connection"])
	c.Store.Schema = file["store"]["schema"]
	c.Store.Aggregates = file["store"]["aggregates"]
	c.Index.Connection, _ = url.Parse(file["index"]["connection"])
	return &c, nil
}

// Open the global configuration file
func Open() (*Config, error) {
	var err error
	if appConfig == nil {
		appConfig, err = LoadFile(*configPath)
	}
	return appConfig, err
}

// The global config
var appConfig *Config
