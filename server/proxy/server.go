package proxy

import (
	"crypto/tls"
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/server/client_conn"
	"github.com/fengleng/log"
	"net"
	"sync"
)

type Server struct {
	Users  map[string]string
	Cfg    *config.Config
	PubKey []byte
	TlsCfg *tls.Config

	CacheShaPassword *sync.Map // 'user@host' -> SHA256(SHA256(PASSWORD))
	running          bool

	listener net.Listener
}

func NewServer(cfg *config.Config) *Server {
	s := new(Server)
	s.Cfg = cfg
	for _, userConfig := range cfg.UserList {
		s.Users[userConfig.User] = userConfig.Password
	}

	//if cfg.PubKeyPath {
	//
	//}

	return s
}

func (s *Server) newClientConn(co net.Conn) *client_conn.ClientConn {
	conn := client_conn.NewClientConn(co)
	conn.SetProxyServer(s)
	return conn
}

func (s *Server) Run() error {
	s.running = true

	// flush counter
	//go s.flushCounter()

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Error("server", "Run", err.Error())
			continue
		}

		go s.onConn(conn)
	}

	return nil
}

func (s *Server) onConn(co net.Conn) {
	conn := s.newClientConn(co)
	err := conn.Handshake()
	if err != nil {
		log.Error("err:%v", err)
		return
	}
}
