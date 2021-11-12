package schema

import (
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/server/backend_node"
	"github.com/juju/errors"
)

type Schema struct {
	cfg         *config.SchemaConfig
	Name        string
	DefaultNode *config.NodeConfig

	NodeMap map[string]*config.NodeConfig
	RuleMap map[string]*config.RuleConfig

	BackendNode map[string]*backend_node.Node
}

func ParseSchemaList(cfgList []config.SchemaConfig) (map[string]*Schema, error) {
	schemaMap := make(map[string]*Schema)
	var err error
	for _, sc := range cfgList {
		_, ok := schemaMap[sc.SchemaName]
		if ok {
			err = errors.Errorf("duplicated schema[%s]", sc.SchemaName)
			return nil, err
		}

		if schema, err := ParseSchema(&sc); err != nil {
			return nil, err
		} else {
			schemaMap[sc.SchemaName] = schema
		}
	}
	return schemaMap, nil
}

func ParseSchema(cfg *config.SchemaConfig) (*Schema, error) {
	var schema Schema
	var err error
	schema.cfg = cfg
	schema.Name = cfg.SchemaName

	schema.NodeMap = make(map[string]*config.NodeConfig)
	for _, nodeCfg := range cfg.NodeList {
		_, ok := schema.NodeMap[nodeCfg.Name]
		if ok {
			err = errors.Errorf("duplicated node[%s]", nodeCfg.Name)
			return nil, err
		}
		schema.NodeMap[nodeCfg.Name] = &nodeCfg
	}

	if backendMap, err := backend_node.ParseNodeList(cfg.NodeList); err != nil {
		return nil, errors.Trace(err)
	} else {
		schema.BackendNode = backendMap
	}

	schema.RuleMap = make(map[string]*config.RuleConfig)
	for _, ruleCfg := range cfg.RuleList {
		_, ok := schema.NodeMap[ruleCfg.TableName]
		if ok {
			err = errors.Errorf("duplicated ruleCfg[%s]", ruleCfg.TableName)
			return nil, err
		}
		schema.RuleMap[ruleCfg.TableName] = &ruleCfg
	}

	schema.DefaultNode = schema.NodeMap[cfg.DefaultNode]
	return &schema, nil
}
