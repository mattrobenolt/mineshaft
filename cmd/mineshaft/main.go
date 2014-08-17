package main

import (
	"github.com/mattrobenolt/mineshaft/api"
	"github.com/mattrobenolt/mineshaft/carbon"
	"github.com/mattrobenolt/mineshaft/config"

	"fmt"
	"log"
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

	go carbon.ListenAndServeAscii(conf.Carbon.Host+":"+conf.Carbon.Port, store)
	go api.ListenAndServe(conf.Http.Host+":"+conf.Http.Port, store)
	select {}
}
