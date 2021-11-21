package plan

import (
	"github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/schema"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/pingcap/errors"
	"strings"
)

func buildSelectPlan(statement sqlparser.Statement, schema *schema.Schema) (*Plan, error) {
	plan := &Plan{}
	//var where *sqlparser.Where
	var err error
	var tableName string

	stmt := statement.(*sqlparser.Select)
	switch v := (stmt.From[0]).(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(v.Expr)
	case *sqlparser.JoinTableExpr:
		if ate, ok := (v.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
			tableName = sqlparser.String(ate.Expr)
		} else {
			tableName = sqlparser.String(v)
		}
	default:
		tableName = sqlparser.String(v)
	}

	plan.Rule = schema.Router.GetRule(tableName, schema.DefaultNode) //根据表名获得分表规则

	//DefaultRuleType 不分库分表==》defaultNode
	//if where {
	//
	//}
	if stmt.Where != nil {
		plan.Criteria = stmt.Where.Expr
	} else {
		plan.Criteria = nil
	}

	if err = plan.calRouteIndexList(); err != nil {
		log.Error("calRouteIndexList err:%v", err)
		return nil, errors.Trace(err)
	}
	err = plan.generateSelectSql(stmt)
	var fromSlave = true
	if 0 < len(stmt.Comments) {
		comment := string(stmt.Comments[0])
		if 0 < len(comment) && strings.ToLower(comment) == MasterComment {
			fromSlave = false
		}
	}
	plan.FromSlave = fromSlave
	return plan, err
}
