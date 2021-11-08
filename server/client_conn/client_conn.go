package client_conn

import (
	"github.com/fengleng/flight/server/proxy"
	"github.com/fengleng/go-mysql-client/mysql"
	"net"
	"sync/atomic"
)

type ClientConn struct {
	c   net.Conn
	pkg *mysql.PacketIO

	proxy *proxy.Server

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

func NewClientConn(co net.Conn) *ClientConn {
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
	return c
}

func (c *ClientConn) SetProxyServer(s *proxy.Server) {
	c.proxy = s
	c.charset = s.Cfg.Charset
	c.collation = s.Cfg.Collation
}

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

func (c *ClientConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *ClientConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}
