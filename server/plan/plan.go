package plan

import (
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

func (plan *Plan) getValueType(valExpr sqlparser.Expr) int {
	switch node := valExpr.(type) {
	case *sqlparser.ColName:
		//remove table name
		if sqlparser.String(node.Qualifier) == plan.Rule.Table {
			node.Qualifier = sqlparser.TableName{}
		}
		if strings.ToLower(sqlparser.String(node.Name)) == plan.Rule.Key {
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

	Criteria            sqlparser.SQLNode
	RouteTableIndexList []int
	RouteNodeIndexList  []int
}

func BuildSchema(statement sqlparser.Statement, schema *schema.Schema) (*Plan, error) {
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

	plan.Rule = schema.Router.GetRule(tableName, schema) //根据表名获得分表规则

	where = stmt.Where
	if where != nil {
		plan.Criteria = where.Expr //路由条件
		err = plan.calRouteIndexs()
		if err != nil {
			Log.Error("BuildSelectPlan err:%v", err)
			return nil, errors.Trace(err)
		}
	}

	//_, err = plan.getTableIndexByExpr(plan.Criteria)
	//if err != nil {
	// return nil, errors.Trace(err)
	//}

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
				return plan.getTableIndexs(node)
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

func (plan *Plan) getTableIndexs(expr sqlparser.Expr) ([]int, error) {
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

//Get the table index of hash shard type
func (plan *Plan) getHashShardTableIndex(expr sqlparser.BoolExpr) ([]int, error) {
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
			return plan.Rule.SubTableIndexs, nil
		case "in":
			return plan.getTableIndexsByTuple(criteria.Right)
		}
	case *sqlparser.RangeCond: //between ... and ...
		return plan.Rule.SubTableIndexs, nil
	default:
		return plan.Rule.SubTableIndexs, nil
	}

	return plan.RouteTableIndexs, nil
}

//Get the table index of range shard type
func (plan *Plan) getRangeShardTableIndex(expr sqlparser.BoolExpr) ([]int, error) {
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
				if criteria.Operator == "<" {
					//调整边界值，当shard[index].start等于criteria.Right 则index--
					index = plan.adjustShardIndex(criteria.Right, index)
				}

				return makeList(0, index+1), nil
			} else {
				index, err = plan.getTableIndexByValue(criteria.Left)
				if err != nil {
					return nil, err
				}
				return makeList(index, len(plan.Rule.SubTableIndexs)), nil
			}
		case ">", ">=":
			if plan.getValueType(criteria.Left) == EID_NODE {
				index, err = plan.getTableIndexByValue(criteria.Right)
				if err != nil {
					return nil, err
				}
				return makeList(index, len(plan.Rule.SubTableIndexs)), nil
			} else { // 10 > id，这种情况
				index, err = plan.getTableIndexByValue(criteria.Left)
				if err != nil {
					return nil, err
				}
				if criteria.Operator == ">" {
					index = plan.adjustShardIndex(criteria.Left, index)
				}
				return makeList(0, index+1), nil
			}
		case "in":
			return plan.getTableIndexsByTuple(criteria.Right)
		case "not in":
			return plan.Rule.SubTableIndexs, nil
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
		if criteria.Operator == "between" { //对应between ...and ...
			if last < start {
				start, last = last, start
			}
			return makeList(start, last+1), nil
		} else { //对应not between ....and
			if last < start {
				start, last = last, start
				start = plan.adjustShardIndex(criteria.To, start)
			} else {
				start = plan.adjustShardIndex(criteria.From, start)
			}

			l1 := makeList(0, start+1)
			l2 := makeList(last, len(plan.Rule.SubTableIndexs))
			return unionList(l1, l2), nil
		}
	default:
		return plan.Rule.SubTableIndexs, nil
	}

	return plan.RouteTableIndexs, nil
}

//Get the table index of date shard type(date_year,date_month,date_day).
func (plan *Plan) getDateShardTableIndex(expr sqlparser.BoolExpr) ([]int, error) {
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
				return makeLeList(index, plan.Rule.SubTableIndexs), nil
			} else {
				index, err = plan.getTableIndexByValue(criteria.Left)
				if err != nil {
					return nil, err
				}
				return makeGeList(index, plan.Rule.SubTableIndexs), nil
			}
		case ">", ">=":
			if plan.getValueType(criteria.Left) == EID_NODE {
				index, err = plan.getTableIndexByValue(criteria.Right)
				if err != nil {
					return nil, err
				}
				return makeGeList(index, plan.Rule.SubTableIndexs), nil
			} else { // 10 > id，这种情况
				index, err = plan.getTableIndexByValue(criteria.Left)
				if err != nil {
					return nil, err
				}
				return makeLeList(index, plan.Rule.SubTableIndexs), nil
			}
		case "in":
			return plan.getTableIndexsByTuple(criteria.Right)
		case "not in":
			l, err := plan.getTableIndexsByTuple(criteria.Right)
			if err != nil {
				return nil, err
			}
			return plan.notList(l), nil
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
			return makeBetweenList(start, last, plan.Rule.SubTableIndexs), nil
		} else { //对应not between ....and
			l := makeBetweenList(start, last, plan.Rule.SubTableIndexs)
			return plan.notList(l), nil
		}
	default:
		return plan.Rule.SubTableIndexs, nil
	}

	return plan.RouteTableIndexs, nil
}

func interList(l1 []int, l2 []int) []int {
	if len(l1) == 0 || len(l2) == 0 {
		return []int{}
	}

	l3 := make([]int, 0, len(l1)+len(l2))
	var i = 0
	var j = 0
	for i < len(l1) && j < len(l2) {
		if l1[i] == l2[j] {
			l3 = append(l3, l1[i])
			i++
			j++
		} else if l1[i] < l2[j] {
			i++
		} else {
			j++
		}
	}

	return l3
}

// l1 | l2
func unionList(l1 []int, l2 []int) []int {
	if len(l1) == 0 {
		return l2
	} else if len(l2) == 0 {
		return l1
	}

	l3 := make([]int, 0, len(l1)+len(l2))

	var i = 0
	var j = 0
	for i < len(l1) && j < len(l2) {
		if l1[i] < l2[j] {
			l3 = append(l3, l1[i])
			i++
		} else if l1[i] > l2[j] {
			l3 = append(l3, l2[j])
			j++
		} else {
			l3 = append(l3, l1[i])
			i++
			j++
		}
	}

	if i != len(l1) {
		l3 = append(l3, l1[i:]...)
	} else if j != len(l2) {
		l3 = append(l3, l2[j:]...)
	}

	return l3
}

// l1 - l2
func differentList(l1 []int, l2 []int) []int {
	if len(l1) == 0 {
		return []int{}
	} else if len(l2) == 0 {
		return l1
	}

	l3 := make([]int, 0, len(l1))

	var i = 0
	var j = 0
	for i < len(l1) && j < len(l2) {
		if l1[i] < l2[j] {
			l3 = append(l3, l1[i])
			i++
		} else if l1[i] > l2[j] {
			j++
		} else {
			i++
			j++
		}
	}

	if i != len(l1) {
		l3 = append(l3, l1[i:]...)
	}

	return l3
}

//计算表下标和node下标
func (plan *Plan) calRouteIndexs() error {
	var err error
	//nodesCount := len(plan.Rule.NodeList)

	//if plan.Rule.Type == DefaultRuleType {
	//	plan.RouteNodeIndexs = []int{0}
	//	return nil
	//}
	//if plan.Criteria == nil { //如果没有分表条件，则是全子表扫描
	//	if plan.Rule.Type != DefaultRuleType {
	//		golog.Error("Plan", "calRouteIndexs", "plan have no criteria", 0,
	//			"type", plan.Rule.Type)
	//		return errors.ErrNoCriteria
	//	}
	//}

	switch criteria := plan.Criteria.(type) {
	//case sqlparser.Values: //代表insert中values
	//	plan.RouteTableIndexList, err = plan.getInsertTableIndex(criteria)
	//	if err != nil {
	//		return err
	//	}
	//	plan.RouteNodeIndexList = plan.TindexsToNindexs(plan.RouteTableIndexs)
	//	return nil
	case sqlparser.Expr:
		plan.RouteTableIndexList, err = plan.getTableIndexByExpr(criteria)
		if err != nil {
			return err
		}
		//plan.RouteNodeIndexList = plan.TindexsToNindexs(plan.RouteTableIndexs)
		return nil
		//default:
		//	plan.RouteTableIndexs = plan.Rule.SubTableIndexs
		//	plan.RouteNodeIndexs = makeList(0, nodesCount)
		//	return nil
	}
	return err
}
