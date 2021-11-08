package client_conn

import (
	"crypto/tls"
	"github.com/fengleng/flight/config"
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/backend_node"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/pingcap/errors"
	"net"
	"os"
	"sync"
	"time"
)

type Server struct {
	Users  map[string]string
	Cfg    *config.Config
	PubKey []byte
	TlsCfg *tls.Config

	BackEndNode map[string]*backend_node.Node

	CacheShaPassword *sync.Map // 'user@host' -> SHA256(SHA256(PASSWORD))
	running          bool

	listener net.Listener
}

func NewServer(cfg *config.Config) *Server {
	s := new(Server)
	s.Cfg = cfg
	s.Users = make(map[string]string)
	for _, userConfig := range cfg.UserList {
		s.Users[userConfig.User] = userConfig.Password
	}

	s.parseCharset(cfg)
	s.BackEndNode = s.parseNodes(cfg)

	var err error
	s.listener, err = net.Listen("tcp", cfg.Addr)

	if err != nil {
		StdLog.Error("err:%v", err)
		os.Exit(config.EXISTCODEINITPROXYSERVER)
	}

	StdLog.Info("server running:%v", cfg.Addr)
	return s
}

func (s *Server) parseNodes(cfg *config.Config) map[string]*backend_node.Node {
	backendNode := make(map[string]*backend_node.Node)
	for _, v := range cfg.Nodes {
		_, ok := backendNode[v.Name]
		if ok {
			StdLog.Error("duplicate node [%s]", v.Name)
			os.Exit(config.EXISTCODEINITPROXYSERVER)
		}
		node, err := parseNode(v)
		if err != nil {
			StdLog.Error("err:%v", err)
			os.Exit(config.EXISTCODEINITPROXYSERVER)
		}
		backendNode[v.Name] = node
	}
	return backendNode
}

func parseNode(cfg config.NodeConfig) (*backend_node.Node, error) {
	var err error
	n := new(backend_node.Node)
	n.Cfg = cfg

	n.DownAfterNoAlive = time.Duration(cfg.DownAfterNoAlive) * time.Second
	err = n.ParseMaster(cfg.Master)
	if err != nil {
		return nil, err
	}
	err = n.ParseSlave(cfg.Slave)
	if err != nil {
		return nil, err
	}

	n.Online = true
	//go n.CheckNode()

	return n, nil
}

func (s *Server) parseCharset(cfg *config.Config) {
	var charset = mysql.DEFAULT_CHARSET
	if cfg.Charset != "" {
		charset = cfg.Charset
	}
	cs, ok := mysql.Charsets[charset]
	if !ok {
		StdLog.Error("invalid charset %s", charset)
		os.Exit(config.EXISTCODEINITPROXYSERVER)
	}

	var collationId = mysql.CollationNames[cs]
	if cfg.Collation != 0 {
		collationId = cfg.Collation
	}

	_, ok = mysql.Collations[collationId]
	if !ok {
		StdLog.Error("invalid collationId %v", collationId)
		os.Exit(config.EXISTCODEINITPROXYSERVER)
	}
	cfg.Charset = charset
	cfg.Collation = collationId
}

func (s *Server) newClientConn(co net.Conn) *ClientConn {
	conn := NewClientConn(co, s)
	//conn.SetProxyServer(s)
	return conn
}

func (s *Server) Run() {
	s.running = true

	// flush counter
	//go s.flushCounter()

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			Log.Error("server, Run %v",err)
			continue
		}

		go s.onConn(conn)
	}
}

func (s *Server) onConn(co net.Conn) {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			Log.Error("%v", errors.AddStack(err))
		}
	}()

	conn := s.newClientConn(co)
	if err := conn.Handshake(); err != nil {
		Log.Error("err:%v", err)
		return
	}
	conn.Run()
}
