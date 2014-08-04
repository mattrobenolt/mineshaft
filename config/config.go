package config

import (
	"github.com/vaughan0/go-ini"

	"io"
	"log"
	"os"
	"strings"
)

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

func Load(input io.Reader) *Config {
	file, err := ini.Load(input)
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

func LoadFile(path string) *Config {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("config: ", err)
	}
	return Load(file)
}
