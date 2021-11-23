package client_conn

import (
	"fmt"
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/flight/server/plan"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/fengleng/go-mysql-client/backend"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/fengleng/log"
	"github.com/pingcap/errors"
	"strings"
	"sync"
	"time"
)

/*处理query语句*/
func (c *ClientConn) handleQuery(sql string) (err error) {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			Log.Error("%v", errors.AddStack(err))
		}
		//_ =c.Close()
	}()

	sql = strings.Trim(sql, ";")
	HandResult, isHand, isNotWriteResult, err := c.PreHand(sql)
	if err != nil {
		Log.Error("server parse %v", errors.Trace(err))
		return err
	}
	if isHand {
		if !isNotWriteResult {
			err := c.writeResultset(HandResult.Status, HandResult.Resultset)
			if err != nil {
				Log.Error("server parse %v", errors.Trace(err))
				return err
			}
		}
		return nil
	}

	if c.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	var stmt sqlparser.Statement
	stmt, err = sqlparser.Parse(sql) //解析sql语句,得到的stmt是一个interface
	if err != nil {
		Log.Error("server parse %v", errors.Trace(err))
		return err
	}

	switch v := stmt.(type) {
	case *sqlparser.Select:
		return c.handleSelect(v, nil)
	case *sqlparser.Insert:
		return c.handleExec(stmt, nil)
	case *sqlparser.Update:
		return c.handleExec(stmt, nil)
	case *sqlparser.Delete:
		return c.handleExec(stmt, nil)
	//case *sqlparser.Replace:
	//	return c.handleExec(stmt, nil)
	case *sqlparser.Set:
		return c.handleSet(v, sql)
	case *sqlparser.Begin:
		return c.handleBegin()
	case *sqlparser.Commit:
		return c.handleCommit()
	case *sqlparser.Rollback:
		return c.handleRollback()
	//case *sqlparser.Admin:
	//	if c.user == "root" {
	//		return c.handleAdmin(v)
	//	}
	//	return fmt.Errorf("statement %T not support now", stmt)
	//case *sqlparser.AdminHelp:
	//	if c.user == "root" {
	//		return c.handleAdminHelp(v)
	//	}
	//	return fmt.Errorf("statement %T not support now", stmt)
	case *sqlparser.Use:
		return c.handleUseDB(sqlparser.String(v.DBName))
	//case *sqlparser.SimpleSelect:
	//	return c.handleSimpleSelect(v)
	//case *sqlparser.Truncate:
	//	return c.handleExec(stmt, nil)
	default:
		return fmt.Errorf("statement %T not support now", stmt)
	}

	return nil
}

func (c *ClientConn) closeShardConns(conns map[string]*backend.Conn, rollback bool) {
	if c.isInTransaction() {
		return
	}

	for _, co := range conns {
		if rollback {
			err := co.Rollback()
			if err != nil {
				log.Error("%s rollback", err)
			}
		}
		co.Close()
	}
}

func (c *ClientConn) executeInMultiNodes(exePlan *plan.Plan, args []interface{}) ([]*mysql.Result, error) {
	var err error
	if exePlan == nil || len(exePlan.RouteNodeIndexList) == 0 {
		return nil, my_errors.ErrNoRouteNode
	}

	conns, err := c.getShardConns(exePlan)

	defer c.closeShardConns(conns, err != nil && c.isInTransaction())
	if err != nil {
		return nil, errors.Trace(err)
	}

	var wg sync.WaitGroup
	wg.Add(len(conns))

	resultCount := 0
	for _, sqlSlice := range exePlan.RewrittenSqlList {
		resultCount += len(sqlSlice)
	}

	rs := make([]interface{}, resultCount)

	f := func(rs []interface{}, i int, sqlList []string, co *backend.Conn) {
		var state string
		for _, v := range sqlList {
			startTime := time.Now().UnixNano()
			r, err := co.Execute(v, args...)
			if err != nil {
				state = "ERROR"
				rs[i] = err
			} else {
				state = "OK"
				rs[i] = r
			}
			execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
			log.Info("%s %.1fms - %s->%s", state, execTime, c.c.RemoteAddr(), c.db)
			i++
		}
		wg.Done()
	}

	offset := 0
	for nodeName, conn := range conns {
		sqlList := exePlan.RewrittenSqlList[nodeName]
		go f(rs, offset, sqlList, conn)
		offset += len(sqlList)
	}
	wg.Wait()

	r := make([]*mysql.Result, resultCount)
	for i, v := range rs {
		if e, ok := v.(error); ok {
			err = e
			break
		}
		if rs[i] != nil {
			r[i] = rs[i].(*mysql.Result)
		}
	}

	return r, err
}
