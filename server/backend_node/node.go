package backend_node

import (
	"fmt"
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/go-mysql-client/backend"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/pingcap/errors"
	"sync"
	"time"
)

type Node struct {
	Master *backend.DB

	sync.RWMutex
	SlaveList        []*backend.DB
	DownAfterNoAlive time.Duration

	Cfg    config.NodeConfig
	DbName string
	Online bool
}

func (n *Node) GetDb(fromSlave bool) *backend.DB {
	if fromSlave {
		return n.SlaveList[0]
	} else {
		return n.Master
	}
}

func ParseNodeList(cfgList []config.NodeConfig, schemaName string) (map[string]*Node, error) {
	var err error
	var backNodeMap = make(map[string]*Node)
	for _, nc := range cfgList {
		_, ok := backNodeMap[nc.Name]
		if ok {
			err = errors.Errorf("duplicated node[%s]", nc.Name)
			return nil, err
		}
		if node, err := ParseNode(nc, schemaName); err != nil {
			return nil, errors.Trace(err)
		} else {
			backNodeMap[nc.Name] = node
		}

	}
	return backNodeMap, nil
}

func ParseNode(cfg config.NodeConfig, schemaName string) (*Node, error) {
	var err error
	n := new(Node)
	n.Cfg = cfg
	n.DbName = schemaName

	n.DownAfterNoAlive = time.Duration(cfg.DownAfterNoAlive) * time.Second

	if err = n.ParseMaster(cfg.Master); err != nil {
		return nil, errors.Trace(err)
	}

	if err = n.ParseSlave(cfg.SlaveList); err != nil {
		return nil, errors.Trace(err)
	}

	n.Online = true
	//go n.CheckNode()

	return n, nil
}

func (n *Node) ParseMaster(masterStr string) error {
	var err error
	if len(masterStr) == 0 {
		return my_errors.ErrNoMasterConn
	}
	master, err := backend.Open(masterStr, n.Cfg.User, n.Cfg.Password, n.DbName)
	n.Master = master

	return err
}

// ParseSlave slaveStr(127.0.0.1:3306@2,192.168.0.12:3306@3)
func (n *Node) ParseSlave(slaveList []string) error {
	for _, slaveStr := range slaveList {
		slave, err := backend.Open(slaveStr, n.Cfg.User, n.Cfg.Password, n.DbName)
		if err != nil {
			return errors.Trace(err)
		}
		n.SlaveList = append(n.SlaveList, slave)
	}

	return nil
}

func (n *Node) UseDb(dbName string) error {
	var command = fmt.Sprintf("use %s", dbName)

	if !n.Online {
		return errors.Errorf("node[%s] have been done", n.Cfg.Name)
	}
	_, err := n.Master.Execute(command)
	if err != nil {
		return errors.Trace(err)
	}

	for _, slave := range n.SlaveList {
		_, err := slave.Execute(command)
		if err != nil {
			return errors.Trace(err)
		}
	}
	n.DbName = dbName

	return nil
}

func (n *Node) Execute(command string, fromSlave bool, args ...interface{}) (*mysql.Result, error) {
	if fromSlave {
		return n.SlaveList[0].Execute(command, args...)
	} else {
		return n.Master.Execute(command, args...)
	}
}

func (n *Node) String() string {
	return n.Cfg.Name
}

func (n *Node) GetMasterConn() (*backend.Conn, error) {
	db := n.Master
	if db == nil {
		return nil, my_errors.ErrNoMasterConn
	}
	if db.State() != "up" {
		return nil, my_errors.ErrMasterDown
	}

	return db.GetConn()
}

func (n *Node) GetSlaveConn() (*backend.Conn, error) {
	n.Lock()
	db := n.SlaveList[0]
	n.Unlock()

	if db == nil {
		return nil, my_errors.ErrNoSlaveDB
	}
	if db.State() != "up" {
		return nil, my_errors.ErrMasterDown
	}

	return db.GetConn()
}
