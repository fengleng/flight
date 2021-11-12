package client_conn

import (
	"fmt"
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server"
	"github.com/fengleng/go-common/core/hack"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/pingcap/errors"
	"net"
	"sync/atomic"
)

type ClientConn struct {
	c   net.Conn
	pkg *mysql.PacketIO

	proxy *server.Server

	connectionId uint32
	status       uint16
	capability   uint32

	collation mysql.CollationId
	charset   string

	user           string
	db             string
	authPluginName string

	salt                []byte
	cachingSha2FullAuth bool
	closed              bool

	lastInsertId int64
	affectedRows int64
}

var DEFAULT_CAPABILITY uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
	mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
	mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION

var baseConnId uint32 = 10000

func NewClientConn(co net.Conn, s *server.Server) *ClientConn {
	c := new(ClientConn)
	tcpConn := co.(*net.TCPConn)

	//SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that data is sent as soon as possible after a Write.
	//I set this option true.
	_ = tcpConn.SetNoDelay(true)
	c.c = tcpConn
	c.pkg = mysql.NewPacketIO(co)
	c.pkg.Sequence = 0

	c.connectionId = atomic.AddUint32(&baseConnId, 1)
	c.status = mysql.SERVER_STATUS_AUTOCOMMIT

	c.salt, _ = mysql.RandomBuf(20)
	//c.txConns = make(map[*backend.Node]*backend.BackendConn)

	c.closed = false

	c.charset = mysql.DEFAULT_CHARSET
	c.collation = mysql.DEFAULT_COLLATION_ID

	//c.stmtId = 0
	//c.stmts = make(map[uint32]*Stmt)
	c.proxy = s
	return c
}

//func (c *ClientConn) SetProxyServer(s *Server) {
//	c.proxy = s
//	c.charset = s.Cfg.Charset
//	c.collation = s.Cfg.Collation
//}

func (c *ClientConn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.writePacket(data)
}

func (c *ClientConn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacket(data)
}

func (c *ClientConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *ClientConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *ClientConn) Close() error {
	if c.closed {
		return nil
	}

	_ = c.c.Close()

	c.closed = true

	return nil
}

func (c *ClientConn) clean() {
	//if c.txConns != nil && len(c.txConns) > 0 {
	//	for _, co := range c.txConns {
	//		co.Close()
	//	}
	//}
}

func (c *ClientConn) Run() {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			Log.Error("%v", errors.AddStack(err))
		}
		_ = c.Close()
	}()
	defer c.clean()
	if err := c.Handshake(); err != nil {
		Log.Error("err:%v", err)
		_ = c.writeError(mysql.NewDefaultError(mysql.ER_DBACCESS_DENIED_ERROR, c.user, c.db))
		return
	}

	for {
		if data, err := c.readPacket(); err != nil {
			Log.Error("%v", errors.AddStack(err))
			return
		} else {
			if err := c.dispatch(data); err != nil {
				Log.Error("ClientConn Run %v, %d", err, c.connectionId)
				_ = c.writeError(err)
				if err == mysql.ErrBadConn {
					_ = c.Close()
				}
			}
		}

		if c.closed {
			return
		}

		c.pkg.Sequence = 0
	}
}

func (c *ClientConn) dispatch(data []byte) error {
	//c.proxy.counter.IncrClientQPS()
	cmd := data[0]
	data = data[1:]

	switch cmd {
	//case mysql.COM_QUIT:
	//	c.handleRollback()
	//	c.Close()
	//	return nil
	case mysql.COM_QUERY:
		return c.handleQuery(hack.String(data))
	case mysql.COM_PING:
		return c.writeOK(nil)
	//case mysql.COM_INIT_DB:
	//	return c.handleUseDB(hack.String(data))
	//case mysql.COM_FIELD_LIST:
	//	return c.handleFieldList(data)
	//case mysql.COM_STMT_PREPARE:
	//	return c.handleStmtPrepare(hack.String(data))
	//case mysql.COM_STMT_EXECUTE:
	//	return c.handleStmtExecute(data)
	//case mysql.COM_STMT_CLOSE:
	//	return c.handleStmtClose(data)
	//case mysql.COM_STMT_SEND_LONG_DATA:
	//	return c.handleStmtSendLongData(data)
	//case mysql.COM_STMT_RESET:
	//	return c.handleStmtReset(data)
	//case mysql.COM_SET_OPTION:
	//	return c.writeEOF(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		Log.Error(msg)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

}

func (c *ClientConn) writePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	return c.pkg.WritePacketBatch(total, data, direct)
}

func (c *ClientConn) writeEOFBatch(total []byte, status uint16, direct bool) ([]byte, error) {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacketBatch(total, data, direct)
}

func (c *ClientConn) writeResultset(status uint16, r *mysql.Resultset) error {
	c.affectedRows = int64(-1)
	total := make([]byte, 0, 4096)
	data := make([]byte, 4, 512)
	var err error

	columnLen := mysql.PutLengthEncodedInt(uint64(len(r.Fields)))

	data = append(data, columnLen...)
	total, err = c.writePacketBatch(total, data, false)
	if err != nil {
		return err
	}

	for _, v := range r.Fields {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	total, err = c.writeEOFBatch(total, status, false)
	if err != nil {
		return err
	}

	for _, v := range r.RowDatas {
		data = data[0:4]
		data = append(data, v...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	total, err = c.writeEOFBatch(total, status, true)
	total = nil
	if err != nil {
		return err
	}

	return nil
}
