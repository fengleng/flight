package plan

import (
	"fmt"
	"github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/flight/server/router"
	"github.com/fengleng/flight/server/schema"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/pingcap/errors"
)

func buildUpdatePlan(statement sqlparser.Statement, schema *schema.Schema) (*Plan, error) {
	plan := &Plan{}

	stmt := statement.(*sqlparser.Update)
	plan.Rule = schema.Router.GetRule(sqlparser.String(stmt.TableExprs), schema.DefaultNode)
	if err := plan.checkUpdateExprs(stmt.Exprs); err != nil {
		return nil, err
	}

	if stmt.Where != nil {
		plan.Criteria = stmt.Where.Expr
	} else {
		plan.Criteria = nil
	}
	if err := plan.calRouteIndexList(); err != nil {
		log.Error("calRouteIndexList err:%v", err)
		return nil, errors.Trace(err)
	}

	if plan.Rule.Type != router.DefaultRuleType && len(plan.RouteTableIndexList) == 0 {
		log.Error("Route BuildUpdatePlan %v", my_errors.ErrNoCriteria.Error())
		return nil, my_errors.ErrNoCriteria
	}
	if err := plan.checkUpdateTable(stmt); err != nil {
		return nil, errors.Trace(err)
	}

	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err := plan.generateUpdateSql(stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (plan *Plan) checkUpdateTable(update *sqlparser.Update) error {
	if len(update.TableExprs) != 1 {
		return my_errors.ErrUpdateTooComplex
	}
	if len(update.TableExprs) == 1 {
		_, ok := update.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !ok {
			return my_errors.ErrUpdateTooComplex
		}
	}
	return nil
}

func (plan *Plan) generateUpdateSql(stmt sqlparser.Statement) error {
	sqlList := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Update)
	if ok == false {
		return my_errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexList) == 0 {
		return my_errors.ErrNoRouteNode
	}
	if len(plan.RouteNodeIndexList) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := plan.Rule.NodeList[0]
		sqlList[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexList)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)

			inBuf := sqlparser.NewTrackedBuffer(nil)
			node.Format(inBuf)
			node1, err := sqlparser.Parse(inBuf.String())
			if err != nil {
				return errors.Trace(err)
			}
			node2 := node1.(*sqlparser.Update)
			TableExpr, ok := node2.TableExprs[0].(*sqlparser.AliasedTableExpr)
			if !ok {
				return my_errors.ErrUpdateTooComplex
			}
			tableName, ok := TableExpr.Expr.(sqlparser.TableName)
			if !ok {
				return my_errors.ErrUpdateTooComplex
			}
			node2.TableExprs[0] = &sqlparser.AliasedTableExpr{
				Expr: &sqlparser.TableName{
					Name:      sqlparser.NewTableIdent(fmt.Sprintf("%s_%04d", sqlparser.String(tableName.Name), plan.RouteTableIndexList[i])),
					Qualifier: sqlparser.NewTableIdent(sqlparser.String(tableName.Qualifier)),
				},
				As:    TableExpr.As,
				Hints: TableExpr.Hints,
			}
			node2.Format(buf)

			tableIndex := plan.RouteTableIndexList[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := plan.Rule.NodeList[nodeIndex]
			if _, ok := sqlList[nodeName]; !ok {
				sqlList[nodeName] = make([]string, 0, tableCount)
			}
			sqlList[nodeName] = append(sqlList[nodeName], buf.String())
		}

	}
	plan.RewrittenSqlList = sqlList
	return nil
}
