package schema

import (
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/server/backend_node"
	"github.com/fengleng/flight/server/router"
	"github.com/juju/errors"
)

type Schema struct {
	cfg         *config.SchemaConfig
	Name        string
	DefaultNode *config.NodeConfig

	NodeMap map[string]*config.NodeConfig
	//TableCfgMap map[string]*config.TableConfig
	RuleMap     map[string]*router.Rule
	TableCfgMap map[string]*config.TableConfig

	Router *router.Router

	BackendNode        map[string]*backend_node.Node
	DefaultBackendNode *backend_node.Node
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
	for _, nodeCfg := range cfg.NodeCfgList {
		_, ok := schema.NodeMap[nodeCfg.Name]
		if ok {
			err = errors.Errorf("duplicated node[%s]", nodeCfg.Name)
			return nil, err
		}
		schema.NodeMap[nodeCfg.Name] = &nodeCfg
	}

	if backendMap, err := backend_node.ParseNodeList(cfg.NodeCfgList, schema.Name); err != nil {
		return nil, errors.Trace(err)
	} else {
		schema.BackendNode = backendMap
	}

	if n, ok := schema.NodeMap[cfg.DefaultNode]; !ok {
		err = errors.Errorf("default node[%s] not exit [%v]", cfg.DefaultNode, cfg.NodeList)
		return nil, err
	} else {
		schema.DefaultNode = n
	}
	schema.DefaultBackendNode, err = backend_node.ParseNode(*schema.NodeMap[cfg.DefaultNode], cfg.SchemaName)
	if r, err := router.ParseRouter(cfg.TableList); err != nil {
		return nil, errors.Trace(err)
	} else {
		schema.Router = r
	}

	return &schema, nil
}
