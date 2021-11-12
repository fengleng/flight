package main

import (
	"flag"
	"github.com/fengleng/flight/config"
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/proxy"
)

//flight
var configFile = flag.String("config", "etc/flight.yaml", "kingshard config file")

func main() {

	if len(*configFile) == 0 {
		StdLog.Error("must use a config file")
		return
	}
	var err error
	var cfg *config.Config
	if cfg, err = config.ParseConfig(*configFile); err != nil {
		StdLog.Error("ParseConfigFile:%s %v", *configFile, err)
		return
	}

	var proxySrv *proxy.Proxy
	if proxySrv, err = proxy.NewProxy(cfg); err != nil {
		Log.Error("NewProxy %v", err)
		return
	}
	if err := proxySrv.Run(); err != nil {
		Log.Error("proxySrv.Run %v", err)
		return
	}

}
