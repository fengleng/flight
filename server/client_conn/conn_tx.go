package client_conn

import (
	"github.com/fengleng/flight/server/backend_node"
	"github.com/fengleng/flight/server/wrap_conn"
	"github.com/fengleng/go-mysql-client/mysql"
)

func (c *ClientConn) isInTransaction() bool {
	return c.status&mysql.SERVER_STATUS_IN_TRANS > 0 ||
		!c.isAutoCommit()
}

func (c *ClientConn) isAutoCommit() bool {
	return c.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

func (c *ClientConn) handleBegin() error {
	//make(map[*backend_node.Node]*wrap_conn.Conn)
	c.txConns = make(map[*backend_node.Node]*wrap_conn.Conn, len(c.srv.BackEndNode))
	//for _, node := range c.srv.BackEndNode {
	//	conn, err := node.Master.GetConn()
	//	if err != nil {
	//		return errors.Trace(err)
	//	}
	//	c.txConns[node] = conn
	//}

	for _, co := range c.txConns {
		if err := co.Begin(); err != nil {
			return err
		}
	}
	c.status |= mysql.SERVER_STATUS_IN_TRANS
	return c.writeOK(nil)
}

func (c *ClientConn) handleCommit() (err error) {
	if err := c.commit(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *ClientConn) handleRollback() (err error) {
	if err := c.rollback(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *ClientConn) commit() (err error) {
	c.status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range c.txConns {
		if e := co.Commit(); e != nil {
			err = e
		}
		co.Close()
	}

	c.txConns = make(map[*backend_node.Node]*wrap_conn.Conn)
	return
}

func (c *ClientConn) rollback() (err error) {
	c.status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range c.txConns {
		if e := co.Rollback(); e != nil {
			err = e
		}
		co.Close()
	}

	c.txConns = make(map[*backend_node.Node]*wrap_conn.Conn)
	return
}
