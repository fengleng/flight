package client_conn

import (
	"github.com/fengleng/go-mysql-client/backend"
	"github.com/fengleng/log"
	. "github.com/pingcap/check"
	"testing"
	"time"
)

type testT1 struct {
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testT1{})

func (s *testT1) TestResult(c *C) {
	//go-mysql-client.
	db, err := backend.Open("127.0.0.1:9999", "root", "root", "")
	c.Assert(err, IsNil)
	result, err := db.Execute("select @@version_comment limit 1;")
	c.Assert(err, IsNil)
	v, err := result.GetString(0, 0)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "MySQL Community Server (GPL)")
}

func (s *testT1) TestUseDb(c *C) {
	//go-mysql-client.
	db, err := backend.Open("127.0.0.1:9999", "root", "root", "")
	c.Assert(err, IsNil)
	//result, err := db.Execute("use biz;")
	//c.Assert(err,IsNil)
	//v,err := result.GetString(0,0)
	//println(result)

	result, err := db.Execute("show databases;")
	c.Assert(err, IsNil)
	//v,err := result.GetString(0,0)
	for _, f := range result.Resultset.Fields {
		println(string(f.Data))
		println(string(f.Schema))
		println(string(f.Table))
		println(string(f.OrgTable))
		println(string(f.Name))
		println(string(f.OrgName))
	}
	println(result)
}

func TestLog(t *testing.T) {
	log.Info("%v", "fsdafasf")
	time.Sleep(time.Second)
}
