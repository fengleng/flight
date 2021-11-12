package test

import (
	"testing"
	"time"
)

func TestT1(t *testing.T) {
	StdLog = NewConsoleLogger(CfgOptionSkip(3), LogLevelCfgOption(DEBUG))
	StdLog.Debug("fsdfsd")
	StdLog.Info("fsdfsd")
	StdLog.Warn("fsdfsd")
	StdLog.Error("fsdfsd")
	StdLog.Fatal("fsdfsd")
	time.Sleep(10 * time.Second)
}
