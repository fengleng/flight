package backend_node

import (
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/go-mysql-client/backend"
	"github.com/pingcap/errors"
	"time"
)

type Node struct {
	Master           *backend.DB
	Slave            []*backend.DB
	DownAfterNoAlive time.Duration

	Cfg config.NodeConfig

	Online bool
}

func ParseNodeList(cfgList []config.NodeConfig) (map[string]*Node, error) {
	var err error
	var backNodeMap = make(map[string]*Node)
	for _, nc := range cfgList {
		_, ok := backNodeMap[nc.Name]
		if ok {
			err = errors.Errorf("duplicated node[%s]", nc.Name)
			return nil, err
		}
		if node, err := parseNode(nc); err != nil {
			return nil, errors.Trace(err)
		} else {
			backNodeMap[nc.Name] = node
		}

	}
	return backNodeMap, nil
}

func parseNode(cfg config.NodeConfig) (*Node, error) {
	var err error
	n := new(Node)
	n.Cfg = cfg

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
	master, err := backend.Open(masterStr, n.Cfg.User, n.Cfg.Password, "")
	n.Master = master

	return err
}

// ParseSlave slaveStr(127.0.0.1:3306@2,192.168.0.12:3306@3)
func (n *Node) ParseSlave(slaveList []string) error {
	//var db *DB
	//var weight int
	//var err error
	//
	//if len(slaveStr) == 0 {
	//	return nil
	//}
	//slaveStr = strings.Trim(slaveStr, SlaveSplit)
	//slaveArray := strings.Split(slaveStr, SlaveSplit)
	//count := len(slaveArray)
	//n.Slave = make([]*DB, 0, count)
	//n.SlaveWeights = make([]int, 0, count)
	//
	////parse addr and weight
	//for i := 0; i < count; i++ {
	//	addrAndWeight := strings.Split(slaveArray[i], WeightSplit)
	//	if len(addrAndWeight) == 2 {
	//		weight, err = strconv.Atoi(addrAndWeight[1])
	//		if err != nil {
	//			return err
	//		}
	//	} else {
	//		weight = 1
	//	}
	//	n.SlaveWeights = append(n.SlaveWeights, weight)
	//	if db, err = n.OpenDB(addrAndWeight[0]); err != nil {
	//		return err
	//	}
	//	n.Slave = append(n.Slave, db)
	//}
	//n.InitBalancer()
	return nil
}
