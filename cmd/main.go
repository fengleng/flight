package main

import (
	"flag"
	"github.com/fengleng/flight/config"
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/client_conn"
)

//flight
var configFile = flag.String("config", "etc/flight.yaml", "kingshard config file")

func main() {
	if len(*configFile) == 0 {
		StdLog.Error("must use a config file")
		return
	}
	cfg, err := config.ParseConfigFile(*configFile)
	if err != nil {
		StdLog.Error("ParseConfigFile:%s %v", *configFile, err)
		return
	}
	proxyServer := client_conn.NewServer(cfg)
	proxyServer.Run()
	//if err != nil {
	//	StdLog.Error("err:%v", err)
	//	os.Exit(1)
	//}
}
