package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"testing"
)

func TestCfgTest(t *testing.T) {
	//data, err := ioutil.ReadFile("./flight.yaml")
	//if err != nil {
	//	t.Fatal(err)
	//}

	var cfg *Config
	//if err := yaml.Unmarshal(data, &cfg); err != nil {
	//	t.Log(err)
	//}
	//
	//for _, p := range cfg.SchemaPath {
	//	data2, err := ioutil.ReadFile(p)
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	var sc SchemaConfig
	//	err = yaml.Unmarshal(data2, &sc)
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	cfg.SchemaList = append(cfg.SchemaList,sc)
	//}
	var err error
	data2, err := ioutil.ReadFile("../test/files/node/node1.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var nc NodeConfig
	err = yaml.Unmarshal(data2, &nc)

	cfg, err = ParseConfig("../test/files/flight.yaml")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(cfg)
}
