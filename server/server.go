package server

import (
	"crypto/tls"
	"github.com/fengleng/flight/config"
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/backend_node"
	"github.com/fengleng/flight/server/schema"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/pingcap/errors"
	"net"
	"sync"
)

type Server struct {
	Users  map[string]string
	Cfg    *config.Config
	PubKey []byte
	TlsCfg *tls.Config

	SchemaMap map[string]*schema.Schema

	BackEndNode map[string]*backend_node.Node

	CacheShaPassword *sync.Map // 'user@host' -> SHA256(SHA256(PASSWORD))
	running          bool

	Listener net.Listener
}

func NewServer(cfg *config.Config) (*Server, error) {
	var err error
	s := new(Server)
	s.Cfg = cfg
	s.Users = make(map[string]string)
	for _, userConfig := range cfg.UserList {
		s.Users[userConfig.User] = userConfig.Password
	}

	if err := s.parseCharset(cfg); err != nil {
		return nil, errors.Trace(err)
	}

	if err := s.parseSchemaList(cfg.SchemaList); err != nil {
		return nil, errors.Trace(err)
	}

	if s.Listener, err = net.Listen("tcp", cfg.Addr); err != nil {
		return nil, errors.Trace(err)
	}

	StdLog.Info("server running:%v", cfg.Addr)
	return s, nil
}

func (s *Server) parseSchemaList(cfgList []config.SchemaConfig) error {

	if schemaMap, err := schema.ParseSchemaList(cfgList); err != nil {
		return errors.Trace(err)
	} else {
		s.SchemaMap = schemaMap
	}

	return nil
}

func (s *Server) parseCharset(cfg *config.Config) (err error) {
	var charset = mysql.DEFAULT_CHARSET
	if cfg.Charset != "" {
		charset = cfg.Charset
	}
	cs, ok := mysql.Charsets[charset]
	if !ok {
		err = errors.Errorf("invalid charset %s", charset)
		return err
	}

	var collationId = mysql.CollationNames[cs]
	if cfg.CollationId != 0 {
		collationId = cfg.CollationId
	}

	_, ok = mysql.Collations[collationId]
	if !ok {
		err = errors.Errorf("invalid collationId %v", collationId)
		return err
	}
	cfg.Charset = charset
	cfg.CollationId = collationId
	return nil
}
