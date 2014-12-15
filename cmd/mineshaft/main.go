package main

import (
	"github.com/mattrobenolt/mineshaft/api"
	"github.com/mattrobenolt/mineshaft/carbon"
	"github.com/mattrobenolt/mineshaft/config"
	log "github.com/mattrobenolt/mineshaft/logging"

	"fmt"
	"runtime"
)

func printBanner() {
	fmt.Print(`
 ███▄ ▄███▓ ██▓ ███▄    █ ▓█████   ██████  ██░ ██  ▄▄▄        █████▒▄▄▄█████▓
▓██▒▀█▀ ██▒▓██▒ ██ ▀█   █ ▓█   ▀ ▒██    ▒ ▓██░ ██▒▒████▄    ▓██   ▒ ▓  ██▒ ▓▒
▓██    ▓██░▒██▒▓██  ▀█ ██▒▒███   ░ ▓██▄   ▒██▀▀██░▒██  ▀█▄  ▒████ ░ ▒ ▓██░ ▒░
▒██    ▒██ ░██░▓██▒  ▐▌██▒▒▓█  ▄   ▒   ██▒░▓█ ░██ ░██▄▄▄▄██ ░▓█▒  ░ ░ ▓██▓ ░
▒██▒   ░██▒░██░▒██░   ▓██░░▒████▒▒██████▒▒░▓█▒░██▓ ▓█   ▓██▒░▒█░      ▒██▒ ░
░ ▒░   ░  ░░▓  ░ ▒░   ▒ ▒ ░░ ▒░ ░▒ ▒▓▒ ▒ ░ ▒ ░░▒░▒ ▒▒   ▓▒█░ ▒ ░      ▒ ░░
░  ░      ░ ▒ ░░ ░░   ░ ▒░ ░ ░  ░░ ░▒  ░ ░ ▒ ░▒░ ░  ▒   ▒▒ ░ ░          ░
░      ░    ▒ ░   ░   ░ ░    ░   ░  ░  ░   ░  ░░ ░  ░   ▒    ░ ░      ░
       ░    ░           ░    ░  ░      ░   ░  ░  ░      ░  ░

`)
}

func main() {
	printBanner()

	runtime.GOMAXPROCS(runtime.NumCPU())

	conf, err := config.Open()
	if err != nil {
		panic(err)
	}
	log.Println(conf)

	store, err := conf.OpenStore()
	if err != nil {
		panic(err)
	}
	defer store.Close()

	if conf.CarbonAscii.Enabled {
		go carbon.ListenAndServeAscii(conf.CarbonAscii.Host+":"+conf.CarbonAscii.Port, store)
	}
	if conf.CarbonPickle.Enabled {
		go carbon.ListenAndServePickle(conf.CarbonPickle.Host+":"+conf.CarbonPickle.Port, store)
	}
	if conf.CarbonProtobuf.Enabled {
		go carbon.ListenAndServeProtobuf(conf.CarbonProtobuf.Host+":"+conf.CarbonProtobuf.Port, store)
	}

	go api.ListenAndServe(conf.Http.Host+":"+conf.Http.Port, store)
	select {}
}
