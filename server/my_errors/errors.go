package my_errors

import "github.com/pingcap/errors"

var (
	ErrNoMasterConn  = errors.NewNoStackError("no master connection")
	ErrNoSlaveConn   = errors.NewNoStackError("no slave connection")
	ErrNoDefaultNode = errors.NewNoStackError("no default node")
	ErrNoMasterDB    = errors.NewNoStackError("no master database")
	ErrNoSlaveDB     = errors.NewNoStackError("no slave database")
	ErrNoDatabase    = errors.NewNoStackError("no database")

	ErrMasterDown    = errors.NewNoStackError("master is down")
	ErrSlaveDown     = errors.NewNoStackError("slave is down")
	ErrDatabaseClose = errors.NewNoStackError("database is close")
	ErrConnIsNil     = errors.NewNoStackError("connection is nil")
	ErrBadConn       = errors.NewNoStackError("connection was bad")
	ErrIgnoreSQL     = errors.NewNoStackError("ignore this sql")

	ErrAddressNull     = errors.NewNoStackError("address is nil")
	ErrInvalidArgument = errors.NewNoStackError("argument is invalid")
	ErrInvalidCharset  = errors.NewNoStackError("charset is invalid")
	ErrCmdUnsupport    = errors.NewNoStackError("command unsupport")

	ErrLocationsCount    = errors.NewNoStackError("locations count is not equal")
	ErrNoCriteria        = errors.NewNoStackError("plan have no criteria")
	ErrNoRouteNode       = errors.NewNoStackError("no route node")
	ErrResultNil         = errors.NewNoStackError("result is nil")
	ErrSumColumnType     = errors.NewNoStackError("sum column type error")
	ErrSelectInInsert    = errors.NewNoStackError("select in insert not allowed")
	ErrInsertInMulti     = errors.NewNoStackError("insert in multi node")
	ErrUpdateInMulti     = errors.NewNoStackError("update in multi node")
	ErrDeleteInMulti     = errors.NewNoStackError("delete in multi node")
	ErrDeleteTableformat = errors.NewNoStackError("delete table format")
	ErrReplaceInMulti    = errors.NewNoStackError("replace in multi node")
	ErrExecInMulti       = errors.NewNoStackError("exec in multi node")
	ErrTransInMulti      = errors.NewNoStackError("transaction in multi node")

	ErrNoPlan           = errors.NewNoStackError("statement have no plan")
	ErrNoPlanRule       = errors.NewNoStackError("statement have no plan rule")
	ErrUpdateKey        = errors.NewNoStackError("routing key in update expression")
	ErrStmtConvert      = errors.NewNoStackError("statement fail to convert")
	ErrExprConvert      = errors.NewNoStackError("expr fail to convert")
	ErrConnNotEqual     = errors.NewNoStackError("the length of conns not equal sqls")
	ErrKeyOutOfRange    = errors.NewNoStackError("shard key not in key range")
	ErrMultiShard       = errors.NewNoStackError("insert or replace has multiple shard targets")
	ErrIRNoColumns      = errors.NewNoStackError("insert or replace must specify columns")
	ErrIRNoShardingKey  = errors.NewNoStackError("insert or replace not contain sharding key")
	ErrColsLenNotMatch  = errors.NewNoStackError("insert or replace cols and values length not match")
	ErrDateIllegal      = errors.NewNoStackError("date format illegal")
	ErrDateRangeIllegal = errors.NewNoStackError("date range format illegal")
	ErrDateRangeCount   = errors.NewNoStackError("date range count is not equal")
	ErrSlaveExist       = errors.NewNoStackError("slave has exist")
	ErrSlaveNotExist    = errors.NewNoStackError("slave has not exist")
	ErrBlackSqlExist    = errors.NewNoStackError("black sql has exist")
	ErrBlackSqlNotExist = errors.NewNoStackError("black sql has not exist")
	ErrInsertTooComplex = errors.NewNoStackError("insert is too complex")
	ErrUpdateTooComplex = errors.NewNoStackError("update is too complex")
	ErrSQLNULL          = errors.NewNoStackError("sql is null")

	ErrInternalServer = errors.NewNoStackError("internal server error")
)
