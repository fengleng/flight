package config

import (
	"testing"
)

func TestCfgTest(t *testing.T) {

	var cfg *Config

	cfg, err := ParseConfig("../etc/flight.yaml")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(cfg)
}
