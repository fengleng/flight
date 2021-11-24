package client_conn

import (
	"fmt"
	"github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/backend_node"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/flight/server/wrap_conn"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/fengleng/go-mysql-client/mysql"
	"strings"
	"time"
)

func (c *ClientConn) handleSet(stmt *sqlparser.Set, sql string) (err error) {
	if len(stmt.Exprs) != 1 && len(stmt.Exprs) != 2 {
		return fmt.Errorf("must set one item once, not %s", sqlparser.String(stmt))
	}

	//log the SQL
	startTime := time.Now().UnixNano()
	defer func() {
		var state string
		if err != nil {
			state = "ERROR"
		} else {
			state = "OK"
		}
		execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
		log.Info("%s %.1fms - %s->%s", state, execTime, c.c.RemoteAddr(), c.db)

	}()

	k := stmt.Exprs[0].Name.Name.String()
	switch strings.ToUpper(k) {
	case `AUTOCOMMIT`, `@@AUTOCOMMIT`, `@@SESSION.AUTOCOMMIT`:
		return c.handleSetAutoCommit(stmt.Exprs[0].Expr)
	case `NAMES`,
		`CHARACTER_SET_RESULTS`, `@@CHARACTER_SET_RESULTS`, `@@SESSION.CHARACTER_SET_RESULTS`,
		`CHARACTER_SET_CLIENT`, `@@CHARACTER_SET_CLIENT`, `@@SESSION.CHARACTER_SET_CLIENT`,
		`CHARACTER_SET_CONNECTION`, `@@CHARACTER_SET_CONNECTION`, `@@SESSION.CHARACTER_SET_CONNECTION`:
		if len(stmt.Exprs) == 2 {
			//SET NAMES 'charset_name' COLLATE 'collation_name'
			return c.handleSetNames(stmt.Exprs[0].Expr, stmt.Exprs[1].Expr)
		}
		return c.handleSetNames(stmt.Exprs[0].Expr, nil)
	default:
		log.Error("ClientConn handleSet command not supported %d sql:%s",
			c.connectionId, sql)
		return my_errors.ErrCmdUnsupport
	}
}

func (c *ClientConn) handleSetAutoCommit(expr sqlparser.Expr) error {
	val := expr.(*sqlparser.SQLVal)
	flag := sqlparser.String(val)
	flag = strings.Trim(flag, "'`\"")
	// autocommit允许为 0, 1, ON, OFF, "ON", "OFF", 不允许"0", "1"
	//if flag == `0` || flag == `1` {
	//	_, ok := val.(sqlparser.NumVal)
	//	if !ok {
	//		return fmt.Errorf("set autocommit error")
	//	}
	//}
	switch strings.ToUpper(flag) {
	case `1`, `ON`:
		c.status |= mysql.SERVER_STATUS_AUTOCOMMIT
		if c.status&mysql.SERVER_STATUS_IN_TRANS > 0 {
			c.status &= ^mysql.SERVER_STATUS_IN_TRANS
		}
		for _, co := range c.txConns {
			if e := co.SetAutoCommit(1); e != nil {
				co.Close()
				c.txConns = make(map[*backend_node.Node]*wrap_conn.Conn)
				return fmt.Errorf("set autocommit error, %v", e)
			}
			co.Close()
		}
		c.txConns = make(map[*backend_node.Node]*wrap_conn.Conn)
	case `0`, `OFF`:
		c.status &= ^mysql.SERVER_STATUS_AUTOCOMMIT
	default:
		return fmt.Errorf("invalid autocommit flag %s", flag)
	}

	return c.writeOK(nil)
}

func (c *ClientConn) handleSetNames(n1, n2 sqlparser.Expr) error {
	var cid mysql.CollationId
	var ok bool

	ch := n1.(*sqlparser.SQLVal)
	ci := n2.(*sqlparser.SQLVal)
	value := sqlparser.String(ch)
	value = strings.Trim(value, "'`\"")

	charset := strings.ToLower(value)
	if charset == "null" {
		return c.writeOK(nil)
	}
	if ci == nil {
		if charset == "default" {
			charset = mysql.DEFAULT_CHARSET
		}
		cid, ok = mysql.CharsetIds[charset]
		if !ok {
			return fmt.Errorf("invalid charset %s", charset)
		}
	} else {
		collate := sqlparser.String(ci)
		collate = strings.Trim(collate, "'`\"")
		collate = strings.ToLower(collate)
		cid, ok = mysql.CollationNames[collate]
		if !ok {
			return fmt.Errorf("invalid collation %s", collate)
		}
	}
	c.charset = charset
	c.collation = cid

	return c.writeOK(nil)
}
