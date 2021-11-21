package plan

import (
	"github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/flight/server/schema"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/pingcap/errors"
)

func buildDeletePlan(statement *sqlparser.Delete, schema *schema.Schema) (*Plan, error) {
	plan := &Plan{}
	var where *sqlparser.Where
	var err error
	var tableName string

	stmt := statement

	if len(stmt.TableExprs) == 1 {
		tableNode, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if ok {
			tableNameNode, ok := tableNode.Expr.(*sqlparser.TableName)
			if ok {
				tableName = sqlparser.String(tableNameNode.Name)
			} else {
				return nil, my_errors.ErrDeleteTableformat
			}

		} else {
			return nil, my_errors.ErrDeleteTableformat
		}
	} else {
		return nil, my_errors.ErrDeleteTableformat
	}

	plan.Rule = schema.Router.GetRule(tableName, schema.DefaultNode) //根据表名获得分表规则

	plan.Criteria = where

	if err = plan.calRouteIndexList(); err != nil {
		log.Error("calRouteIndexList err:%v", err)
		return nil, errors.Trace(err)
	}

	if err = plan.generateDeleteSql(stmt); err != nil {
		return plan, err
	}

	return plan, err
}

func (plan *Plan) generateDeleteSql(stmt *sqlparser.Delete) error {
	sqlList := make(map[string][]string)
	node := stmt
	if len(plan.RouteNodeIndexList) == 0 {
		return my_errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexList) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := plan.Rule.NodeList[0]
		sqlList[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexList)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			cloneBuf := sqlparser.NewTrackedBuffer(nil)
			node.Format(cloneBuf)

			node1, err := sqlparser.Parse(cloneBuf.String())
			if err != nil {
				return err
			}

			node2 := node1.(*sqlparser.Delete)
			tableNode := node2.TableExprs[0].(*sqlparser.AliasedTableExpr)
			tableNameNode := tableNode.Expr.(*sqlparser.TableName)

			node2.TableExprs[0] = &sqlparser.AliasedTableExpr{
				Expr: &sqlparser.TableName{
					Name:      sqlparser.NewTableIdent(sqlparser.String(tableNameNode.Name)),
					Qualifier: sqlparser.NewTableIdent(sqlparser.String(tableNameNode.Qualifier)),
				},
				As:    tableNode.As,
				Hints: tableNode.Hints,
			}
			node2.Format(buf)
			tableIndex := plan.RouteTableIndexList[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := plan.Rule.NodeList[nodeIndex]
			if _, ok := sqlList[nodeName]; ok == false {
				sqlList[nodeName] = make([]string, 0, tableCount)
			}
			sqlList[nodeName] = append(sqlList[nodeName], buf.String())
		}

	}
	plan.RewrittenSqlList = sqlList
	return nil
}
