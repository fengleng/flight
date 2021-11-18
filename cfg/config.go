package cfg

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)
import (
	"github.com/fengleng/flight/log"
)

type Config struct {
	Addr       string       `yaml:"addr"`
	LogLevel   string       `yaml:"log_level"`
	SchemaPath []string     `yaml:"schema_path"`
	LogPath    string       `yaml:"log_path"`
	UserList   []UserConfig `yaml:"user_list"`
	SchemaList []SchemaConfig
}

type SchemaConfig struct {
	//UserList []UserConfig `yaml:"user_list"`
	NodeList   []NodeConfig `yaml:"node_list"`
	SchemaName string       `yaml:"schema_name"`
	//NodeList []NodeConfig `yaml:"node_list"`
	DefaultNode string `yaml:"default_node"`

	RuleList []RuleConfig `yaml:"rule_list"`
	//TableList []TableConfig `yaml:"table_list"`
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
		log.Fatal("parse config err :%v", err)
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatal("parse config err :%v", err)
		return nil, err
	}

	for _, p := range cfg.SchemaPath {
		data2, err := ioutil.ReadFile(p)
		if err != nil {
			log.Fatal("parse config err :%v", err)
			return nil, err
		}
		var sc SchemaConfig
		err = yaml.Unmarshal(data2, &sc)
		if err != nil {
			log.Fatal("parse config err :%v", err)
			return nil, err
		}
		cfg.SchemaList = append(cfg.SchemaList, sc)
	}

	return &cfg, nil
}
