package proxy

import (
	"github.com/fengleng/flight/config"
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server"
	"github.com/fengleng/flight/server/client_conn"
	"github.com/pingcap/errors"
	//"net"
)

type Proxy struct {
	//ClientConn *client_conn.ClientConn
	Server  *server.Server
	running bool
}

func NewProxy(cfg *config.Config) (*Proxy, error) {
	if srv, err := server.NewServer(cfg); err != nil {
		return nil, errors.Trace(err)
	} else {
		p := new(Proxy)
		p.Server = srv
		return p, nil
	}
}

func (p *Proxy) Run() error {
	if p.running {
		return errors.Errorf("flight proxy[%s] is running", p.Server.Cfg.Addr)
	}
	p.running = true
	for {
		conn, err := p.Server.Listener.Accept()
		if err != nil {
			Log.Error("server, Run %v", err)
			continue
		}
		if !p.running {
			return nil
		}
		clientConn := client_conn.NewClientConn(conn, p.Server)

		go clientConn.Run()
	}
}
