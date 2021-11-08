package errors

import "github.com/pingcap/errors"

var (
	ErrNoMasterConn = errors.NewNoStackError("no master connection")
	ErrNoSlaveConn  = errors.NewNoStackError("no slave connection")
)
