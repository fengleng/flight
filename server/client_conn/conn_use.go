package client_conn

import (
	"fmt"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/juju/errors"
)

func (c *ClientConn) handleUseDB(dbName string) error {

	if len(dbName) == 0 {
		return fmt.Errorf("must have database, the length of dbName is zero")
	}
	schemaMap := c.srv.SchemaMap
	if len(schemaMap) < 1 {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}
	schema, ok := schemaMap[dbName]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, dbName)
	}
	c.schema = schema

	for _, node := range schema.BackendNode {
		err := node.UseDb(dbName)
		if err != nil {
			return errors.Trace(err)
		}
	}
	c.db = dbName
	return c.writeOK(nil)
}
