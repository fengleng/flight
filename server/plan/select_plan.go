package plan

import (
	"fmt"
	"github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/my_errors"
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

func (plan *Plan) generateSelectSql(stmt sqlparser.Statement) error {
	rewrittenSqlList := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Select)
	if ok == false {
		return my_errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexList) == 0 {
		return my_errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexList) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := plan.Rule.NodeList[0]
		rewrittenSqlList[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexList)
		for i := 0; i < tableCount; i++ {
			tableIndex := plan.RouteTableIndexList[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := plan.Rule.NodeList[nodeIndex]
			selectSql, err := plan.rewriteSelectSql(node, tableIndex)
			if err != nil {
				return errors.Trace(err)
			}
			if _, ok := rewrittenSqlList[nodeName]; ok == false {
				rewrittenSqlList[nodeName] = make([]string, 0, tableCount)
			}
			rewrittenSqlList[nodeName] = append(rewrittenSqlList[nodeName], selectSql)
		}
	}
	plan.RewrittenSqlList = rewrittenSqlList
	return nil
}

//rewrite select sql
func (plan *Plan) rewriteSelectSql(statement *sqlparser.Select, tableIndex int) (string, error) {
	inBuf := sqlparser.NewTrackedBuffer(nil)
	statement.Format(inBuf)
	newSql := inBuf.String()

	node2, err := sqlparser.Parse(newSql)
	if err != nil {
		return "", errors.Trace(err)
	}
	node := node2.(*sqlparser.Select)

	buf := sqlparser.NewTrackedBuffer(nil)
	for _, expr := range node.SelectExprs {
		switch v := expr.(type) {
		case *sqlparser.StarExpr:
			//for shardTable.*,need replace table into shardTable_xxxx.
			if sqlparser.String(v.TableName.Name) == plan.Rule.Table {
				oldName := v.TableName
				v.TableName = sqlparser.TableName{
					Name:      sqlparser.NewTableIdent(fmt.Sprintf("%s_%04d", oldName.Name.String(), tableIndex)),
					Qualifier: sqlparser.NewTableIdent(oldName.Qualifier.String()),
				}
			}
		case *sqlparser.AliasedExpr:
			if colName, ok := v.Expr.(*sqlparser.ColName); ok {
				if sqlparser.String(colName.Qualifier) == plan.Rule.Table {
					oldQualifier := colName.Qualifier
					colName.Qualifier = sqlparser.TableName{
						Name:      sqlparser.NewTableIdent(fmt.Sprintf("%s_%04d", oldQualifier.Name.String(), tableIndex)),
						Qualifier: sqlparser.NewTableIdent(oldQualifier.Qualifier.String()),
					}
					//colName.Format(buf)
				}
			}
		}
	}

	switch v := (node.From[0]).(type) {
	case *sqlparser.AliasedTableExpr:
		switch o := v.Expr.(type) {
		case sqlparser.TableName:
			v.Expr = sqlparser.TableName{
				Name:      sqlparser.NewTableIdent(fmt.Sprintf("%s_%04d", o.Name.String(), tableIndex)),
				Qualifier: sqlparser.NewTableIdent(o.Qualifier.String()),
			}
		}
	}

	node.Format(buf)
	return buf.String(), nil
}
