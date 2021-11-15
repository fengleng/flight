package config

import (
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/go-mysql-client/mysql"
	"github.com/juju/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

type Config struct {
	Addr     string `yaml:"addr"`
	LogLevel string `yaml:"log_level"`

	Charset           string            `yaml:"charset"`
	CollationId       mysql.CollationId `yaml:"collation_id"`
	DefaultAuthMethod string            `yaml:"default_auth_method"`

	LogPath  string       `yaml:"log_path"`
	UserList []UserConfig `yaml:"user_list"`

	NodePath   []string `yaml:"node_path"`
	NodeList   []NodeConfig
	SchemaPath []string `yaml:"schema_path"`
	SchemaList []SchemaConfig
}

type SchemaConfig struct {
	NodeList   []string `yaml:"node_list"`
	SchemaName string   `yaml:"schema_name"`

	NodeCfgList []NodeConfig

	DefaultNode string `yaml:"default_node"`

	RuleList []RuleConfig `yaml:"rule_list"`
}

type RuleConfig struct {
	TableName      string             `yaml:"table_name"`
	Key            string             `yaml:"key"`
	Type           string             `yaml:"type"`
	Location       []int              `yaml:"location"`
	ChildTableList []ChildTableConfig `yaml:"child_table_list"`
}

type ChildTableConfig struct {
	AssociatedKey string `yaml:"associated_key"`
	TableName     string `yaml:"table_name"`
}

type UserConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type NodeConfig struct {
	Name             string `yaml:"name"`
	DownAfterNoAlive int    `yaml:"down_after_noalive"`
	MaxConnsLimit    int    `yaml:"max_conns_limit"`

	User     string `yaml:"user"`
	Password string `yaml:"password"`
	//DbName   string `yaml:"db_name"`

	Master    string   `yaml:"master"`
	SlaveList []string `yaml:"slave_list"`
}

func ParseConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		StdLog.Fatal("parse config err :%v", err)
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		StdLog.Fatal("parse config err :%v", err)
		return nil, err
	}

	for _, p := range cfg.NodePath {
		data2, err := ioutil.ReadFile(p)
		if err != nil {
			StdLog.Fatal("parse config err :%v", err)
			return nil, err
		}
		var nc NodeConfig
		err = yaml.Unmarshal(data2, &nc)
		if err != nil {
			StdLog.Fatal("parse config err :%v", err)
			return nil, err
		}
		nc.Name = strings.Trim(nc.Name, " ")
		if isContainsNode(nc.Name, cfg.NodeList) {
			err := errors.Errorf("node[%s] duplicated", nc.Name)
			StdLog.Fatal("%v", err)
			return nil, err
		}
		cfg.NodeList = append(cfg.NodeList, nc)
	}

	for _, p := range cfg.SchemaPath {
		data2, err := ioutil.ReadFile(p)
		if err != nil {
			StdLog.Fatal("parse config err :%v", err)
			return nil, err
		}
		var sc SchemaConfig
		err = yaml.Unmarshal(data2, &sc)
		if err != nil {
			StdLog.Fatal("parse config err :%v", err)
			return nil, err
		}
		var nodeCfgList []NodeConfig
		for _, nodeName := range sc.NodeList {

			if isContainsNode(nodeName, nodeCfgList) {
				err := errors.Errorf("schema[%s] node[%s] duplicated", sc.SchemaName, nodeName)
				StdLog.Fatal("%v", err)
				return nil, err
			}

			if nodeCfg, ok := findGlobalNodeCfg(nodeName, cfg.NodeList); !ok {
				err := errors.Errorf("schema[%s] node[%s] not exist", sc.SchemaName, nodeName)
				StdLog.Fatal("%v", err)
				return nil, err
			} else {
				sc.NodeCfgList = append(sc.NodeCfgList, nodeCfg)
			}
		}
		cfg.SchemaList = append(cfg.SchemaList, sc)
	}

	return &cfg, nil
}

func isContainsNode(nodeName string, nodeCfgList []NodeConfig) bool {
	nodeName = strings.Trim(nodeName, " ")
	for _, config := range nodeCfgList {
		if nodeName == config.Name {
			return true
		}
	}
	return false
}

func findGlobalNodeCfg(nodeName string, nodeCfgList []NodeConfig) (NodeConfig, bool) {
	nodeName = strings.Trim(nodeName, " ")
	cfg, isFind := NodeConfig{}, false
	for _, config := range nodeCfgList {
		if nodeName == config.Name {
			isFind = true
			cfg = config
			break
		}
	}
	return cfg, isFind
}
