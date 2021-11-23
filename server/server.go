package server

import (
	"crypto/tls"
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/log"
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

	NodeMap     map[string]*config.NodeConfig
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

	if err := s.parseNodeList(cfg.NodeList); err != nil {
		return nil, errors.Trace(err)
	}

	if err := s.parseSchemaList(cfg.SchemaList); err != nil {
		return nil, errors.Trace(err)
	}

	if s.Listener, err = net.Listen("tcp", cfg.Addr); err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("server running:%v", cfg.Addr)

	log.InitLogger(log.NewFileLogger(log.CfgOptionSkip(5)))
	return s, nil
}

func (s *Server) parseNodeList(cfgList []config.NodeConfig) error {
	var err error
	s.NodeMap = make(map[string]*config.NodeConfig)
	for _, nodeCfg := range cfgList {
		_, ok := s.NodeMap[nodeCfg.Name]
		if ok {
			err = errors.Errorf("duplicated node[%s]", nodeCfg.Name)
			return err
		}
		s.NodeMap[nodeCfg.Name] = &nodeCfg
	}

	if backendMap, err := backend_node.ParseNodeList(cfgList, ""); err != nil {
		return errors.Trace(err)
	} else {
		s.BackEndNode = backendMap
	}
	return nil
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
