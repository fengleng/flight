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
	var where *sqlparser.Where

	stmt := statement.(*sqlparser.Update)
	plan.Rule = schema.Router.GetRule(sqlparser.String(stmt.TableExprs), schema.DefaultNode)
	if err := plan.checkUpdateExprs(stmt.Exprs); err != nil {
		return nil, err
	}

	where = stmt.Where
	if where != nil {
		plan.Criteria = where.Expr //路由条件
		err := plan.calRouteIndexList()
		if err != nil {
			log.Error("plan calRouteIndexList %v", err.Error())
			return nil, err
		}
	} else {
		//if shard update without where,send to all nodes and all tables
		plan.RouteTableIndexList = plan.Rule.SubTableIndexList
		plan.RouteNodeIndexList = makeList(0, len(plan.Rule.NodeList))
	}

	if plan.Rule.Type != router.DefaultRuleType && len(plan.RouteTableIndexList) == 0 {
		log.Error("Route BuildUpdatePlan %v", my_errors.ErrNoCriteria.Error())
		return nil, my_errors.ErrNoCriteria
	}
	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err = plan.generateUpdateSql(stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
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
			buf.Fprintf("update %v%v",
				node.Comments,
				node.Table,
			)
			inBuf := sqlparser.NewTrackedBuffer(nil)
			node.Format(inBuf)
			node1, err := sqlparser.Parse(inBuf.String())
			if err != nil {
				return errors.Trace(err)
			}
			node2 := node1.(*sqlparser.Update)

			node2.TableExprs =

				fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			buf.Fprintf(" set %v%v%v%v",
				node.Exprs,
				node.Where,
				node.OrderBy,
				node.Limit,
			)
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]
			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}
			sqls[nodeName] = append(sqls[nodeName], buf.String())
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}
