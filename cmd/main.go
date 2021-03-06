package main

import (
	"flag"
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/proxy"
	"time"
)

//flight
var configFile = flag.String("config", "../etc/flight.yaml", "kingshard config file")

func main() {
	defer func() {
		time.Sleep(1 * time.Second)
	}()
	if len(*configFile) == 0 {
		log.Error("must use a config file")
		return
	}
	var err error
	var cfg *config.Config
	if cfg, err = config.ParseConfig(*configFile); err != nil {
		log.Error("ParseConfigFile:%s %v", *configFile, err)
		return
	}

	var proxySrv *proxy.Proxy
	if proxySrv, err = proxy.NewProxy(cfg); err != nil {
		log.Error("NewProxy %v", err)
		return
	}
	if err := proxySrv.Run(); err != nil {
		log.Error("proxySrv.Run %v", err)
		return
	}

}
