package main

import (
	"flag"
	"log"

	"github.com/BronOS/segdb/internal/app/apiserver"
	"github.com/BurntSushi/toml"
)

var (
	configPath string
	bindAddr   string
)

func init() {
	flag.StringVar(&configPath, "config-path", "configs/config.toml", "path to config file")
	flag.StringVar(&bindAddr, "addr", ":4509", "api server address and port")
}

func main() {
	flag.Parse()

	config := apiserver.NewConfig(bindAddr)
	_, err := toml.DecodeFile(configPath, config)

	if err != nil {
		log.Fatal(err)
	}

	s := apiserver.New(config)

	if err := s.Start(); err != nil {
		log.Fatal(err)
	}
}
