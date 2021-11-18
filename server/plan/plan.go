package plan

import (
	"fmt"
	. "github.com/fengleng/flight/log"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/flight/server/router"
	"github.com/fengleng/flight/server/schema"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/juju/errors"
	"strings"
)

const (
	EID_NODE = iota
	VALUE_NODE
	LIST_NODE
	OTHER_NODE
)

const (
	MasterComment = "/*master*/"
)

func (plan *Plan) getValueType(valExpr sqlparser.Expr) int {
	switch node := valExpr.(type) {
	case *sqlparser.ColName:
		//remove table name
		if sqlparser.String(node.Qualifier) == plan.Rule.Table {
			node.Qualifier = sqlparser.TableName{}
		}
		if node.Name.EqualString(plan.Rule.Key) {
			return EID_NODE //表示这是分片id对应的node
		}
	//case sqlparser.ValTuple:
	//	for _, n := range node {
	//		if plan.getValueType(n) != VALUE_NODE {
	//			return OTHER_NODE
	//		}
	//	}
	//	return LIST_NODE //列表节点
	case *sqlparser.SQLVal: //普通的值节点，字符串值，绑定变量参数
		return VALUE_NODE
	}
	return OTHER_NODE
}

type Plan struct {
	//stmt sqlparser.Statement
	Rule   *router.Rule
	Schema *schema.Schema

	Criteria            sqlparser.Expr
	RouteTableIndexList []int
	RouteNodeIndexList  []int
	RewrittenSqlList    map[string][]string

	FromSlave bool
}

func BuildPlan(statement sqlparser.Statement, schema *schema.Schema) (*Plan, error) {

	switch stmt := statement.(type) {
	//case *sqlparser.Insert:
	//	return r.buildInsertPlan(db, stmt)
	//case *sqlparser.Replace:
	//	return r.buildReplacePlan(db, stmt)
	case *sqlparser.Select:
		return buildSelectPlan(stmt, schema)
		//case *sqlparser.Update:
		//	return r.buildUpdatePlan(db, stmt)
		//case *sqlparser.Delete:
		//	return r.buildDeletePlan(db, stmt)
		//case *sqlparser.Truncate:
		//	return r.buildTruncatePlan(db, stmt)
	}
	return nil, my_errors.ErrNoPlan
}

func buildSelectPlan(statement sqlparser.Statement, schema *schema.Schema) (*Plan, error) {
	plan := &Plan{}
	var where *sqlparser.Where
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
	if plan.Rule.Type != router.DefaultRuleType {
		where = stmt.Where
		if where != nil {
			plan.Criteria = where.Expr //路由条件
			err = plan.calRouteIndexList()
			if err != nil {
				Log.Error("BuildSelectPlan err:%v", err)
				return nil, errors.Trace(err)
			}
		} else {
			//if shard select without where,send to all nodes and all tables
			plan.RouteTableIndexList = plan.Rule.SubTableIndexList
			plan.RouteNodeIndexList = makeList(0, len(plan.Rule.NodeList))
		}
	} else {
		plan.RouteNodeIndexList = []int{0}
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

func (plan *Plan) getTableIndexByExpr(node sqlparser.Expr) ([]int, error) {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		left, err := plan.getTableIndexByExpr(node.Left)
		if err != nil {
			return nil, err
		}
		right, err := plan.getTableIndexByExpr(node.Right)
		if err != nil {
			return nil, err
		}
		return interList(left, right), nil
	case *sqlparser.OrExpr:
		left, err := plan.getTableIndexByExpr(node.Left)
		if err != nil {
			return nil, err
		}
		right, err := plan.getTableIndexByExpr(node.Right)
		if err != nil {
			return nil, err
		}
		return unionList(left, right), nil
	case *sqlparser.ParenExpr: //加上括号的BoolExpr，node.Expr去掉了括号
		return plan.getTableIndexByExpr(node.Expr)
	case *sqlparser.ComparisonExpr:
		switch {
		case sqlparser.StringIn(node.Operator, "=", "<", ">", "<=", ">=", "<=>"):
			left := plan.getValueType(node.Left)
			right := plan.getValueType(node.Right)
			if (left == EID_NODE && right == VALUE_NODE) || (left == VALUE_NODE && right == EID_NODE) {
				return plan.getTableIndexList(node)
			}
			//case sqlparser.StringIn(node.Operator, "in", "not in"):
			//	left := plan.getValueType(node.Left)
			//	right := plan.getValueType(node.Right)
			//	if left == EID_NODE && right == LIST_NODE {
			//		if strings.EqualFold(node.Operator, "in") { //only deal with in expr, it's impossible to process not in here.
			//			plan.InRightToReplace = node
			//		}
			//		return plan.getTableIndexs(node)
			//	}
		}
	}
	return plan.Rule.SubTableIndexList, nil
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
			if sqlparser.String(v.TableName) == plan.Rule.Table {
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
					colName.Format(buf)
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

func (plan *Plan) getTableIndexList(expr sqlparser.Expr) ([]int, error) {
	switch plan.Rule.Type {
	case router.HashRuleType:
		return plan.getHashShardTableIndex(expr)
	case router.RangeRuleType:
		return plan.getRangeShardTableIndex(expr)
	case router.DateYearRuleType, router.DateMonthRuleType, router.DateDayRuleType:
		return plan.getDateShardTableIndex(expr)
	default:
		return plan.Rule.SubTableIndexList, nil
	}
	//return nil, nil
}

func (plan *Plan) getTableIndexByValue(expr sqlparser.Expr) (int, error) {
	var value interface{}
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		value = plan.getBoundValue(v)
	default:
		value = 0
	}

	return plan.Rule.FindTableIndex(value)
}

//Get the table index of hash shard type
func (plan *Plan) getHashShardTableIndex(expr sqlparser.Expr) ([]int, error) {
	var index int
	var err error
	switch criteria := expr.(type) {
	case *sqlparser.ComparisonExpr:
		switch criteria.Operator {
		case "=", "<=>": //=对应的分片
			if plan.getValueType(criteria.Left) == EID_NODE {
				index, err = plan.getTableIndexByValue(criteria.Right)
			} else {
				index, err = plan.getTableIndexByValue(criteria.Left)
			}
			if err != nil {
				return nil, err
			}
			return []int{index}, nil
		case "<", "<=", ">", ">=", "not in":
			return plan.Rule.SubTableIndexList, nil
			//case "in":
			//	return plan.getTableIndexsByTuple(criteria.Right)
		}
	case *sqlparser.RangeCond: //between ... and ...
		return plan.Rule.SubTableIndexList, nil
	default:
		return plan.Rule.SubTableIndexList, nil
	}

	return plan.RouteTableIndexList, nil
}

/*获得valExpr对应的值*/
func (plan *Plan) getBoundValue(valExpr *sqlparser.SQLVal) interface{} {
	buf := sqlparser.NewTrackedBuffer(nil)
	valExpr.Format(buf)
	return buf.String()
	//switch node := valExpr.(type) {
	//case sqlparser.ValTuple: //ValTuple可以是一个slice
	//	if len(node) != 1 {
	//		panic(sqlparser.NewParserError("tuples not allowed as insert values"))
	//	}
	//	// TODO: Change parser to create single value tuples into non-tuples.
	//	return plan.getBoundValue(node[0])
	//case sqlparser.StrVal:
	//	return string(node)
	//case sqlparser.NumVal:
	//	val, err := strconv.ParseInt(string(node), 10, 64)
	//	if err != nil {
	//		panic(sqlparser.NewParserError("%s", err.Error()))
	//	}
	//	return val
	//case sqlparser.ValArg:
	//	panic("Unexpected token")
	//}
	//panic("Unexpected token")
}

//Get the table index of range shard type
func (plan *Plan) getRangeShardTableIndex(expr sqlparser.Expr) ([]int, error) {
	var index int
	var err error
	switch criteria := expr.(type) {
	case *sqlparser.ComparisonExpr:
		switch criteria.Operator {
		case "=", "<=>": //=对应的分片
			if plan.getValueType(criteria.Left) == EID_NODE {
				index, err = plan.getTableIndexByValue(criteria.Right)
			} else {
				index, err = plan.getTableIndexByValue(criteria.Left)
			}
			if err != nil {
				return nil, err
			}
			return []int{index}, nil
		//case "<", "<=":
		//	if plan.getValueType(criteria.Left) == EID_NODE {
		//		index, err = plan.getTableIndexByValue(criteria.Right)
		//		if err != nil {
		//			return nil, err
		//		}
		//		if criteria.Operator == "<" {
		//			//调整边界值，当shard[index].start等于criteria.Right 则index--
		//			index = plan.adjustShardIndex(criteria.Right, index)
		//		}
		//
		//		return makeList(0, index+1), nil
		//	} else {
		//		index, err = plan.getTableIndexByValue(criteria.Left)
		//		if err != nil {
		//			return nil, err
		//		}
		//		return makeList(index, len(plan.Rule.SubTableIndexList)), nil
		//	}
		//case ">", ">=":
		//	if plan.getValueType(criteria.Left) == EID_NODE {
		//		index, err = plan.getTableIndexByValue(criteria.Right)
		//		if err != nil {
		//			return nil, err
		//		}
		//		return makeList(index, len(plan.Rule.SubTableIndexList)), nil
		//	} else { // 10 > id，这种情况
		//		index, err = plan.getTableIndexByValue(criteria.Left)
		//		if err != nil {
		//			return nil, err
		//		}
		//		if criteria.Operator == ">" {
		//			index = plan.adjustShardIndex(criteria.Left, index)
		//		}
		//		return makeList(0, index+1), nil
		//	}
		//case "in":
		//	return plan.getTableIndexsByTuple(criteria.Right)
		case "not in":
			return plan.Rule.SubTableIndexList, nil
		}
	//case *sqlparser.RangeCond:
	//	var start, last int
	//	start, err = plan.getTableIndexByValue(criteria.From)
	//	if err != nil {
	//		return nil, err
	//	}
	//	last, err = plan.getTableIndexByValue(criteria.To)
	//	if err != nil {
	//		return nil, err
	//	}
	//	if criteria.Operator == "between" { //对应between ...and ...
	//		if last < start {
	//			start, last = last, start
	//		}
	//		return makeList(start, last+1), nil
	//	} else { //对应not between ....and
	//		if last < start {
	//			start, last = last, start
	//			start = plan.adjustShardIndex(criteria.To, start)
	//		} else {
	//			start = plan.adjustShardIndex(criteria.From, start)
	//		}
	//
	//		l1 := makeList(0, start+1)
	//		l2 := makeList(last, len(plan.Rule.SubTableIndexList))
	//		return unionList(l1, l2), nil
	//	}
	default:
		return plan.Rule.SubTableIndexList, nil
	}

	return plan.RouteTableIndexList, nil
}

//func (plan *Plan) adjustShardIndex(valExpr sqlparser.Expr, index int) int {
//	value := plan.getBoundValue(valExpr)
//	//生成一个范围的接口,[100,120)
//	s, ok := plan.Rule.Shard.(RangeShard)
//	if !ok {
//		return index
//	}
//	//value是否和shard[index].Start相等
//	if s.EqualStart(value, index) {
//		index--
//		if index < 0 {
//			panic(sqlparser.NewParserError("invalid range sharding"))
//		}
//	}
//	return index
//}

//Get the table index of date shard type(date_year,date_month,date_day).
func (plan *Plan) getDateShardTableIndex(expr sqlparser.Expr) ([]int, error) {
	var index int
	var err error
	switch criteria := expr.(type) {
	case *sqlparser.ComparisonExpr:
		switch criteria.Operator {
		case "=", "<=>": //=对应的分片
			if plan.getValueType(criteria.Left) == EID_NODE {
				index, err = plan.getTableIndexByValue(criteria.Right)
			} else {
				index, err = plan.getTableIndexByValue(criteria.Left)
			}
			if err != nil {
				return nil, err
			}
			return []int{index}, nil
		case "<", "<=":
			if plan.getValueType(criteria.Left) == EID_NODE {
				index, err = plan.getTableIndexByValue(criteria.Right)
				if err != nil {
					return nil, err
				}
				return makeLeList(index, plan.Rule.SubTableIndexList), nil
			} else {
				index, err = plan.getTableIndexByValue(criteria.Left)
				if err != nil {
					return nil, err
				}
				return makeGeList(index, plan.Rule.SubTableIndexList), nil
			}
		case ">", ">=":
			if plan.getValueType(criteria.Left) == EID_NODE {
				index, err = plan.getTableIndexByValue(criteria.Right)
				if err != nil {
					return nil, err
				}
				return makeGeList(index, plan.Rule.SubTableIndexList), nil
			} else { // 10 > id，这种情况
				index, err = plan.getTableIndexByValue(criteria.Left)
				if err != nil {
					return nil, err
				}
				return makeLeList(index, plan.Rule.SubTableIndexList), nil
			}
			//case "in":
			//	return plan.getTableIndexsByTuple(criteria.Right)
			//case "not in":
			//	l, err := plan.getTableIndexsByTuple(criteria.Right)
			//	if err != nil {
			//		return nil, err
			//	}
			//	return plan.notList(l), nil
		}
	case *sqlparser.RangeCond:
		var start, last int
		start, err = plan.getTableIndexByValue(criteria.From)
		if err != nil {
			return nil, err
		}
		last, err = plan.getTableIndexByValue(criteria.To)
		if err != nil {
			return nil, err
		}
		if last < start {
			start, last = last, start
		}
		if criteria.Operator == "between" { //对应between ...and ...
			return makeBetweenList(start, last, plan.Rule.SubTableIndexList), nil
		} else { //对应not between ....and
			l := makeBetweenList(start, last, plan.Rule.SubTableIndexList)
			return plan.notList(l), nil
		}
	default:
		return plan.Rule.SubTableIndexList, nil
	}

	return plan.RouteTableIndexList, nil
}

func (plan *Plan) notList(l []int) []int {
	return differentList(plan.Rule.SubTableIndexList, l)
}

//计算表下标和node下标
func (plan *Plan) calRouteIndexList() error {
	var err error
	nodesCount := len(plan.Rule.NodeList)
	switch criteria := plan.Criteria.(type) {
	case sqlparser.Expr:
		plan.RouteTableIndexList, err = plan.getTableIndexByExpr(criteria)
		if err != nil {
			return err
		}
		if err != nil {
			return errors.Trace(err)
		}
		plan.RouteNodeIndexList = plan.TIndexListToNIndexList(plan.RouteTableIndexList)
		return nil
	default:
		plan.RouteTableIndexList = plan.Rule.SubTableIndexList
		plan.RouteTableIndexList = makeList(0, nodesCount)
		return nil
	}
}

func (plan *Plan) TIndexListToNIndexList(tableIndexList []int) []int {
	count := len(tableIndexList)
	nodeIndexList := make([]int, 0, count)
	for i := 0; i < count; i++ {
		tx := tableIndexList[i]
		nodeIndexList = append(nodeIndexList, plan.Rule.TableToNode[tx])
	}

	return cleanList(nodeIndexList)
}
