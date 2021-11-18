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

func buildInsertPlan(statement sqlparser.Statement, schema *schema.Schema) (*Plan, error) {
	plan := &Plan{}
	plan.Rows = make(map[int]sqlparser.Values)
	stmt := statement.(*sqlparser.Insert)
	if _, ok := stmt.Rows.(sqlparser.SelectStatement); ok {
		return nil, my_errors.ErrSelectInInsert
	}

	if stmt.Columns == nil {
		return nil, my_errors.ErrIRNoColumns
	}

	//根据sql语句的表，获得对应的分片规则
	plan.Rule = schema.Router.GetRule(sqlparser.String(stmt.Table), schema.DefaultNode) //根据表名获得分表规则

	err := plan.GetIRKeyIndex(stmt.Columns)
	if err != nil {
		return nil, err
	}

	if stmt.OnDup != nil {
		err := plan.checkUpdateExprs(sqlparser.UpdateExprs(stmt.OnDup))
		if err != nil {
			return nil, err
		}
	}

	plan.Criteria, err = plan.checkValuesType(stmt.Rows.(sqlparser.Values))
	if err != nil {
		log.Error("router buildInsertPlan %v sql %s", err.Error(), sqlparser.String(statement))
		return nil, err
	}

	err = plan.calRouteIndexList()
	if err != nil {
		log.Error("Route calRouteIndexList %v", err.Error())
		return nil, err
	}

	err = plan.generateInsertSql(stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

//get the insert table index and set plan.Rows
func (plan *Plan) getInsertTableIndex(valList sqlparser.Values) ([]int, error) {
	tableIndexList := make([]int, 0, len(valList))
	rowsToTIndex := make(map[int][]sqlparser.ValTuple)
	for i := 0; i < len(valList); i++ {
		tuple := valList[i]
		if len(tuple) < (plan.KeyIndex + 1) {
			return nil, my_errors.ErrColsLenNotMatch
		}

		tableIndex, err := plan.getTableIndexByValue(tuple[plan.KeyIndex])
		if err != nil {
			return nil, err
		}

		tableIndexList = append(tableIndexList, tableIndex)
		//get the rows insert into this table
		rowsToTIndex[tableIndex] = append(rowsToTIndex[tableIndex], tuple)
	}
	for k, v := range rowsToTIndex {
		plan.Rows[k] = v
	}

	return cleanList(tableIndexList), nil
}

func (plan *Plan) generateInsertSql(stmt sqlparser.Statement) error {
	sqlList := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Insert)
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
		sqlList[nodeName] = []string{buf.String()}
	} else {
		tableCount := len(plan.RouteTableIndexList)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			tableIndex := plan.RouteTableIndexList[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := plan.Rule.NodeList[nodeIndex]
			//深拷贝
			inBuf := sqlparser.NewTrackedBuffer(nil)
			node.Format(inBuf)
			node2, err := sqlparser.Parse(inBuf.String())
			if err != nil {
				return errors.Trace(err)
			}
			node3 := node2.(*sqlparser.Insert)

			node3.Table = sqlparser.TableName{
				Name:      sqlparser.NewTableIdent(fmt.Sprintf("%s_%04d", sqlparser.String(node3.Table), plan.RouteTableIndexList[i])),
				Qualifier: node.Table.Qualifier,
			}
			node3.Format(buf)
			if _, ok := sqlList[nodeName]; !ok {
				sqlList[nodeName] = make([]string, 0, tableCount)
			}
			sqlList[nodeName] = append(sqlList[nodeName], buf.String())
		}

	}
	plan.RewrittenSqlList = sqlList
	return nil
}

func (plan *Plan) checkValuesType(insertRows sqlparser.InsertRows) (sqlparser.Values, error) {
	switch values := insertRows.(type) {
	case sqlparser.Values:
		//Analyze first value of every item in the list
		for i := 0; i < len(values); i++ {
			tuple := values[i]
			result := plan.getValueType(tuple[0])
			if result != VALUE_NODE {
				return nil, my_errors.ErrInsertTooComplex
			}

		}
		return values, nil
	default:
		return nil, my_errors.ErrInsertTooComplex
	}
}

// GetIRKeyIndex find shard key index in insert or replace SQL
// plan.Rule cols must not nil
func (plan *Plan) GetIRKeyIndex(cols sqlparser.Columns) error {
	if plan.Rule == nil {
		return my_errors.ErrNoPlanRule
	}
	plan.KeyIndex = -1
	for i, _ := range cols {
		if cols[i].EqualString(plan.Rule.Key) {
			plan.KeyIndex = i
			break
		}
	}
	if plan.KeyIndex == -1 {
		return my_errors.ErrIRNoShardingKey
	}
	return nil
}

//UpdateExprs is the expression after set
func (plan *Plan) checkUpdateExprs(exprs sqlparser.UpdateExprs) error {
	if plan.Rule.Type == router.DefaultRuleType {
		return nil
	} else if len(plan.Rule.NodeList) == 1 {
		return nil
	}

	for _, e := range exprs {
		if sqlparser.String(e.Name.Name) == plan.Rule.Key {
			return my_errors.ErrUpdateKey
		}
	}
	return nil
}
