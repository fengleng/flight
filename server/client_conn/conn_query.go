package client_conn

import (
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/pingcap/errors"
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

	var stmt sqlparser.Statement
	stmt, err = sqlparser.Parse(sql) //解析sql语句,得到的stmt是一个interface
	if err != nil {
		Log.Error("server parse %v", errors.Trace(err))
		return err
	}

	vv, ok := stmt.(*sqlparser.Select)
	if !ok {
		return mysql.NewDefaultError(mysql.ER_PARSE_ERROR)
	}
	Log.Info("%v", vv)
	node := c.proxy.BackEndNode["biz"]

	result, err := node.Master.Execute(sql)
	if err != nil {
		Log.Error("server parse %v", errors.Trace(err))
		return err
	}
	err = c.writeResultset(result.Status, result.Resultset)
	if err != nil {
		Log.Error("server writeResultSet %v", errors.Trace(err))
		return err
	}

	//switch v := stmt.(type) {
	//case *sqlparser.Select:
	//	return c.handleSelect(v, nil)
	//case *sqlparser.Insert:
	//	return c.handleExec(stmt, nil)
	//case *sqlparser.Update:
	//	return c.handleExec(stmt, nil)
	//case *sqlparser.Delete:
	//	return c.handleExec(stmt, nil)
	//case *sqlparser.Replace:
	//	return c.handleExec(stmt, nil)
	//case *sqlparser.Set:
	//	return c.handleSet(v, sql)
	//case *sqlparser.Begin:
	//	return c.handleBegin()
	//case *sqlparser.Commit:
	//	return c.handleCommit()
	//case *sqlparser.Rollback:
	//	return c.handleRollback()
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
	//case *sqlparser.UseDB:
	//	return c.handleUseDB(v.DB)
	//case *sqlparser.SimpleSelect:
	//	return c.handleSimpleSelect(v)
	//case *sqlparser.Truncate:
	//	return c.handleExec(stmt, nil)
	//default:
	//	return fmt.Errorf("statement %T not support now", stmt)
	//}

	return nil
}
