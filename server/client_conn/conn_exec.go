package client_conn

import (
	"github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/plan"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/pingcap/errors"
)

func (c *ClientConn) handleExec(stmt sqlparser.Statement, args []interface{}) error {
	exePlan, err := plan.BuildPlan(stmt, c.schema)
	if err != nil {
		return errors.Trace(err)
	}

	var rs []*mysql.Result
	rs, err = c.executeInMultiNodes(exePlan, args)
	if err != nil {
		log.Info("handleSelect executeInMultiNodes %v connectId: %d", err, c.connectionId)
		return errors.Trace(err)
	}
	err = c.mergeExecResult(rs)
	if err != nil {
		log.Info("handleSelect mergeSelectResult %v connectId: %d", err, c.connectionId)
		return errors.Trace(err)
	}
	return nil
}

func (c *ClientConn) mergeExecResult(rs []*mysql.Result) error {
	r := new(mysql.Result)
	for _, v := range rs {
		r.Status |= v.Status
		r.AffectedRows += v.AffectedRows
		if r.InsertId == 0 {
			r.InsertId = v.InsertId
		} else if r.InsertId > v.InsertId {
			//last insert id is first gen id for multi row inserted
			//see http://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
			r.InsertId = v.InsertId
		}
	}

	if r.InsertId > 0 {
		c.lastInsertId = int64(r.InsertId)
	}
	c.affectedRows = int64(r.AffectedRows)

	return c.writeOK(r)
}
