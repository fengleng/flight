package client_conn

import (
	"fmt"
	"github.com/fengleng/flight/sqlparser/sqlparser"
)

func (c *ClientConn) handleCreateSchema(stmt *sqlparser.DbDDL, sql string) error {
	if len(sqlparser.String(stmt.DbName)) == 0 {
		return fmt.Errorf("must have database, the length of dbName is zero")
	}
	//schemaMap := c.srv.SchemaMap

	return c.writeOK(nil)
}
