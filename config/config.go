package config

import (
	"github.com/mattrobenolt/mineshaft/aggregate"
	"github.com/mattrobenolt/mineshaft/index"
	"github.com/mattrobenolt/mineshaft/schema"
	"github.com/mattrobenolt/mineshaft/store"
	"github.com/vaughan0/go-ini"

	"flag"
	"log"
	"net/url"
	"os"
)

var configPath = flag.String("f", "/etc/mineshaft/mineshaft.conf", "configuration file")

func init() {
	flag.Parse()
}

type Config struct {
	Carbon struct {
		Host string
		Port string
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
	c := &Config{}
	c.Carbon.Host = file["carbon"]["host"]
	c.Carbon.Port = file["carbon"]["port"]
	c.Http.Host = file["http"]["host"]
	c.Http.Port = file["http"]["port"]
	c.Store.Connection, _ = url.Parse(file["store"]["connection"])
	c.Store.Schema = file["store"]["schema"]
	c.Store.Aggregates = file["store"]["aggregates"]
	c.Index.Connection, _ = url.Parse(file["index"]["connection"])
	return c, nil
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
