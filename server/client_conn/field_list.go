package client_conn

import (
	"bytes"
	"github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/pingcap/errors"
)

func (c *ClientConn) handleFieldList(data []byte) error {
	index := bytes.IndexByte(data, 0x00)
	table := string(data[0:index])
	wildcard := string(data[index+1:])

	if c.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	schema := c.schema
	rule := schema.Router.GetRule(table, c.schema.DefaultNode)

	node, ok := schema.BackendNode[rule.DefaultNode]
	if !ok {
		return my_errors.ErrDefaultNodeNotExist
	}
	wc, err := c.getWrapConn(node, true)
	defer c.closeShardConn(wc, err != nil && c.isInTransaction())
	if err != nil {
		log.Error("err:%v", err)
		return errors.Trace(err)
	}

	if err = wc.UseDB(c.db); err != nil {
		//reset the database to null
		c.db = ""
		return err
	}

	if fs, err := wc.FieldList(table, wildcard); err != nil {
		return err
	} else {
		return c.writeFieldList(c.status, fs)
	}
}

func (c *ClientConn) writeFieldList(status uint16, fs []*mysql.Field) error {
	c.affectedRows = int64(-1)
	var err error
	total := make([]byte, 0, 1024)
	data := make([]byte, 4, 512)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	_, err = c.writeEOFBatch(total, status, true)
	return err
}
