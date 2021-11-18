package router

import (
	"fmt"
	"github.com/fengleng/flight/config"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/juju/errors"
)

var (
	DefaultRuleType   = "default"
	HashRuleType      = "hash"
	ModRuleType       = "mod"
	RangeRuleType     = "range"
	DateYearRuleType  = "date_year"
	DateMonthRuleType = "date_month"
	DateDayRuleType   = "date_day"
	MinMonthDaysCount = 28
	MaxMonthDaysCount = 31
	MonthsCount       = 12
)

type Rule struct {
	cfg config.TableConfig

	Table string

	Key string

	Type string

	ReferenceTable *config.AssociatedTableConfig
	IsAssociated   bool
	//ReferenceKey   string

	DefaultNode string
	NodeList    []string

	SubTableIndexList []int       //SubTableIndexList store all the index of sharding sub-table,sequential
	TableToNode       map[int]int //key is table index, and value is node index

	Shard Shard
}

type Router struct {
	Rules map[string]*Rule
}

func (r *Router) GetRule(tableName string, defaultNode *config.NodeConfig) *Rule {
	if rule, ok := r.Rules[tableName]; ok {
		return rule
	} else {
		return NewDefaultRule(defaultNode.Name)
	}
}

func NewDefaultRule(node string) *Rule {
	return &Rule{
		Type:        DefaultRuleType,
		NodeList:    []string{node},
		Shard:       new(DefaultShard),
		TableToNode: nil,
	}
}

func (r *Rule) FindNode(key interface{}) (string, error) {
	nodeIndex, err := r.FindNodeIndex(key)
	if err != nil {
		return "", err
	}
	return r.NodeList[nodeIndex], nil
}

func (r *Rule) FindNodeIndex(key interface{}) (int, error) {
	tableIndex, err := r.FindTableIndex(key)
	if err != nil {
		return -1, err
	}
	return r.TableToNode[tableIndex], nil
}

func (r *Rule) FindTableIndex(key interface{}) (int, error) {
	return r.Shard.FindForKey(key)
}

func ParseRouter(cfgList []config.TableConfig, cfg *config.SchemaConfig) (router *Router, err error) {
	router = new(Router)
	router.Rules = make(map[string]*Rule)
	for _, tableCfg := range cfgList {
		_, ok := router.Rules[tableCfg.TableName]
		if ok {
			err = errors.Errorf("duplicated tableCfg[%s]", tableCfg.TableName)
			return nil, err
		}
		if tableCfg.DefaultNode == "" {
			tableCfg.DefaultNode = cfg.DefaultNode
		}
		if len(tableCfg.NodeList) == 0 {
			tableCfg.NodeList = cfg.NodeList
		}

		rule, err := ParseRule(tableCfg, cfgList)
		if err != nil {
			return nil, errors.Trace(err)
		}
		router.Rules[tableCfg.TableName] = rule
	}

	return router, err
}

func newRule(cfg config.TableConfig, cfgList []config.TableConfig) (*Rule, error) {
	if cfg.Type != "" && cfg.AssociatedTable != nil {
		return nil, errors.Errorf("rule [%s] type, associatedTable only change one", cfg.TableName)
	}
	if cfg.Type == "" && cfg.AssociatedTable == nil {
		return NewDefaultRule(cfg.DefaultNode), nil
	}

	r := &Rule{
		cfg:   cfg,
		Table: cfg.TableName,
	}

	if cfg.Type == "" && cfg.AssociatedTable != nil {
		if ac, ok := containsTable(cfg, cfgList); !ok {
			return nil, errors.Errorf("[%s] associatedTable [%s] is not found")
		} else {
			r.ReferenceTable = cfg.AssociatedTable
			r.IsAssociated = true
			r.Key = cfg.AssociatedTable.Fk
			r.Type = ac.Type
			//r.ReferenceKey = cfg.AssociatedTable.ReferenceCol
		}
	} else {
		r.Key = cfg.Key
		r.Type = cfg.Type
	}

	r.DefaultNode = cfg.DefaultNode
	r.NodeList = cfg.NodeList
	r.TableToNode = make(map[int]int, 0)
	return r, nil
}

func containsTable(table config.TableConfig, cfgList []config.TableConfig) (config.TableConfig, bool) {
	for _, t := range cfgList {
		if t.TableName == table.AssociatedTable.ReferenceTableName {
			return t, true
		}
	}
	return config.TableConfig{}, false
}

func ParseRule(cfg config.TableConfig, cfgList []config.TableConfig) (*Rule, error) {
	r, err := newRule(cfg, cfgList)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := parseRuleNode(cfg, r); err != nil {
		return nil, errors.Trace(err)
	}

	if err := parseShard(r, cfg); err != nil {
		return nil, err
	}

	return r, nil
}

func parseRuleNode(cfg config.TableConfig, r *Rule) error {
	switch r.Type {
	case HashRuleType, RangeRuleType:
		var sumTables int
		if len(cfg.Locations) != len(r.NodeList) {
			return my_errors.ErrLocationsCount
		}
		for i := 0; i < len(cfg.Locations); i++ {
			for j := 0; j < cfg.Locations[i]; j++ {
				r.SubTableIndexList = append(r.SubTableIndexList, j+sumTables)
				r.TableToNode[j+sumTables] = i
			}
			sumTables += cfg.Locations[i]
		}
	case DateDayRuleType:
		if len(cfg.DateRange) != len(r.NodeList) {
			return my_errors.ErrDateRangeCount
		}
		for i := 0; i < len(cfg.DateRange); i++ {
			dayNumbers, err := ParseDayRange(cfg.DateRange[i])
			if err != nil {
				return err
			}
			currIndexLen := len(r.SubTableIndexList)
			if currIndexLen > 0 && r.SubTableIndexList[currIndexLen-1] >= dayNumbers[0] {
				return my_errors.ErrDateIllegal
			}
			for _, v := range dayNumbers {
				r.SubTableIndexList = append(r.SubTableIndexList, v)
				r.TableToNode[v] = i
			}
		}
	case DateMonthRuleType:
		if len(cfg.DateRange) != len(r.NodeList) {
			return my_errors.ErrDateRangeCount
		}
		for i := 0; i < len(cfg.DateRange); i++ {
			monthNumbers, err := ParseMonthRange(cfg.DateRange[i])
			if err != nil {
				return err
			}
			currIndexLen := len(r.SubTableIndexList)
			if currIndexLen > 0 && r.SubTableIndexList[currIndexLen-1] >= monthNumbers[0] {
				return my_errors.ErrDateIllegal
			}
			for _, v := range monthNumbers {
				r.SubTableIndexList = append(r.SubTableIndexList, v)
				r.TableToNode[v] = i
			}
		}
	case DateYearRuleType:
		if len(cfg.DateRange) != len(r.NodeList) {
			return my_errors.ErrDateRangeCount
		}
		for i := 0; i < len(cfg.DateRange); i++ {
			yearNumbers, err := ParseYearRange(cfg.DateRange[i])
			if err != nil {
				return err
			}
			currIndexLen := len(r.SubTableIndexList)
			if currIndexLen > 0 && r.SubTableIndexList[currIndexLen-1] >= yearNumbers[0] {
				return my_errors.ErrDateIllegal
			}
			for _, v := range yearNumbers {
				r.TableToNode[v] = i
				r.SubTableIndexList = append(r.SubTableIndexList, v)
			}
		}
	}
	return nil
}

func parseShard(r *Rule, cfg config.TableConfig) error {
	switch r.Type {
	case HashRuleType:
		r.Shard = &HashShard{ShardNum: len(r.TableToNode)}
	case RangeRuleType:
		rs, err := ParseNumSharding(cfg.Locations, cfg.TableRowLimit)
		if err != nil {
			return err
		}

		if len(rs) != len(r.TableToNode) {
			return fmt.Errorf("range space %d not equal tables %d", len(rs), len(r.TableToNode))
		}

		r.Shard = &NumRangeShard{Shards: rs}
	case DateDayRuleType:
		r.Shard = &DateDayShard{}
	case DateMonthRuleType:
		r.Shard = &DateMonthShard{}
	case DateYearRuleType:
		r.Shard = &DateYearShard{}
	default:
		r.Shard = &DefaultShard{}
	}

	return nil
}
