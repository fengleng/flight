package backend_node

import (
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/server/errors"
	"github.com/fengleng/go-mysql-client/backend"
	"time"
)

type Node struct {
	Master           *backend.DB
	Slave            []*backend.DB
	DownAfterNoAlive time.Duration

	Cfg config.NodeConfig

	Online bool
}

func (n *Node) ParseMaster(masterStr string) error {
	var err error
	if len(masterStr) == 0 {
		return errors.ErrNoMasterConn
	}
	master, err := backend.Open(masterStr, n.Cfg.User, n.Cfg.Password, n.Cfg.DbName)
	n.Master = master

	return err
}

//slaveStr(127.0.0.1:3306@2,192.168.0.12:3306@3)
func (n *Node) ParseSlave(slaveStr string) error {
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
