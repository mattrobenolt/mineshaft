package config

import (
	"github.com/vaughan0/go-ini"

	"flag"
	"log"
	"os"
	"strings"
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
		Cluster    []string
		Keyspace   string
		Schema     string
		Aggregates string
	}
	Index struct {
		Index string
		Url   string
	}
}

// Load an return a Config object by path
func LoadFile(path string) *Config {
	log.Println("loading config", path)
	fp, err := os.Open(path)
	if err != nil {
		log.Fatal("config: ", err)
	}
	file, err := ini.Load(fp)
	if err != nil {
		panic(err)
	}
	c := &Config{}
	c.Carbon.Host = file["carbon"]["host"]
	c.Carbon.Port = file["carbon"]["port"]
	c.Http.Host = file["http"]["host"]
	c.Http.Port = file["http"]["port"]
	c.Store.Cluster = strings.Split(file["store"]["cluster"], ",")
	c.Store.Keyspace = file["store"]["keyspace"]
	c.Store.Schema = file["store"]["schema"]
	c.Store.Aggregates = file["store"]["aggregates"]
	c.Index.Index = file["index"]["index"]
	c.Index.Url = file["index"]["url"]
	return c
}

// Open the global configuration file
func Open() *Config {
	if appConfig == nil {
		appConfig = LoadFile(*configPath)
	}
	return appConfig
}

// The global config
var appConfig *Config
