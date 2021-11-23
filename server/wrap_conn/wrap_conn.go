package wrap_conn

import "github.com/fengleng/go-mysql-client/backend"

type Conn struct {
	*backend.Conn
	Db *backend.DB
}
