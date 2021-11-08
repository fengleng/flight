package test

import (
	"testing"
	"time"
)

//var yyy = NewConsoleLogger()

func TestT2(t *testing.T) {
	t.Log(name)
	t.Log(dir)
	g := Log
	//g := NewConsoleLogger()
	g.Error("dfsafasf")

	StdLog.Fatal("sdfsadfas")

	go func() {
		for {
			time.Sleep(time.Second)
			g.Error("dfsafasf")
			StdLog.Info("vsdfasdfasdfa")
		}
	}()
	time.Sleep(time.Second * 60)
	//yyy.Error("dfsafasf")
}
