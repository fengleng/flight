package schema

import (
	"github.com/fengleng/flight/config"
	"github.com/fengleng/go-mysql-client/backend"
	"time"
)

//func parseNode(cfg *config.NodeConfig) * {
//
//}

type Node struct {
	Master           *backend.DB
	Slave            []*backend.DB
	DownAfterNoAlive time.Duration

	Cfg config.NodeConfig

	Online bool
}
