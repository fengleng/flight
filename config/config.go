package config

import (
	"github.com/fengleng/go-mysql-client/mysql"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// Config 整个代理配置文件
type Config struct {
	Addr     string       `yaml:"addr"`
	UserList []UserConfig `yaml:"user_list"`

	DefaultAuthMethod string `yaml:"default_auth_method"`
	PubKeyPath        string `yaml:"pub_key_path"`
	CaCrtPath         string `yaml:"ca_crt_path"`
	ServerKeyPath     string `yaml:"server_key_path"`
	ServerCrtPath     string `yaml:"server_crt_path"`

	LogPath  string
	LogLevel string `yaml:"log_level"`

	Charset   string            `yaml:"proxy_charset"`
	Collation mysql.CollationId `yaml:"collation"`
	Nodes     []NodeConfig      `yaml:"nodes"`

	SchemaList []SchemaConfig `yaml:"schema_list"`
}

// UserConfig user_list对应的配置
type UserConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// NodeConfig node节点对应的配置
type NodeConfig struct {
	Name             string `yaml:"name"`
	DownAfterNoAlive int    `yaml:"down_after_noalive"`
	MaxConnNum       int    `yaml:"max_conns_limit"`

	User     string `yaml:"user"`
	Password string `yaml:"password"`

	Master string `yaml:"master"`
	Slave  string `yaml:"slave"`
}

// SchemaConfig schema对应的结构体
type SchemaConfig struct {
	User      string        `yaml:"user"`
	Nodes     []string      `yaml:"nodes"`
	Default   string        `yaml:"default"` //default node
	ShardRule []ShardConfig `yaml:"shard"`   //route rule
}

// ShardConfig range,hash or date
type ShardConfig struct {
	DB            string   `yaml:"db"`
	Table         string   `yaml:"table"`
	Key           string   `yaml:"key"`
	Nodes         []string `yaml:"nodes"`
	Locations     []int    `yaml:"locations"`
	Type          string   `yaml:"type"`
	TableRowLimit int      `yaml:"table_row_limit"`
	DateRange     []string `yaml:"date_range"`
}

func ParseConfigData(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ParseConfigFile(fileName string) (*Config, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	//configFileName = fileName

	return ParseConfigData(data)
}
