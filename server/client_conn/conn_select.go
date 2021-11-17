package client_conn

import (
	"fmt"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/fengleng/flight/sqlparser/tidbparser/dependency/util/hack"
	"github.com/fengleng/go-mysql-client/mysql"
	"strconv"
	"strings"
)

const (
	MasterComment    = "/*master*/"
	SumFunc          = "sum"
	CountFunc        = "count"
	MaxFunc          = "max"
	MinFunc          = "min"
	LastInsertIdFunc = "last_insert_id"
	FUNC_EXIST       = 1
)

var funcNameMap = map[string]int{
	"sum":            FUNC_EXIST,
	"count":          FUNC_EXIST,
	"max":            FUNC_EXIST,
	"min":            FUNC_EXIST,
	"last_insert_id": FUNC_EXIST,
}

func (c *ClientConn) handleFieldList(data []byte) error {
	//index := bytes.IndexByte(data, 0x00)
	//table := string(data[0:index])
	//wildcard := string(data[index+1:])
	//
	//if c.schema == nil {
	//	return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	//}
	//
	//nodeName := c.schema.rule.GetRule(c.db, table).Nodes[0]
	//
	//n := c.proxy.GetNode(nodeName)
	//co, err := c.getBackendConn(n, false)
	//defer c.closeConn(co, false)
	//if err != nil {
	//	return err
	//}
	//
	//if err = co.UseDB(c.db); err != nil {
	//	//reset the database to null
	//	c.db = ""
	//	return err
	//}
	//
	//if fs, err := co.FieldList(table, wildcard); err != nil {
	//	return err
	//} else {
	//	return c.writeFieldList(c.status, fs)
	//}
	return nil
}

func (c *ClientConn) writeFieldList(status uint16, fs []*mysql.Field) error {
	c.affectedRows = int64(-1)
	var err error
	total := make([]byte, 0, 1024)
	data := make([]byte, 4, 512)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	_, err = c.writeEOFBatch(total, status, true)
	return err
}

func (c *ClientConn) mergeSelectResult(rs []*mysql.Result, stmt *sqlparser.Select) error {
	var r *mysql.Result
	var err error

	if len(stmt.GroupBy) == 0 {
		r, err = c.buildSelectOnlyResult(rs, stmt)
	} else {
		//group by
		r, err = c.buildSelectGroupByResult(rs, stmt)
	}
	if err != nil {
		return err
	}

	_ = c.sortSelectResult(r.Resultset, stmt)
	//to do, add log here, sort may error because order by key not exist in resultset fields

	if err := c.limitSelectResult(r.Resultset, stmt); err != nil {
		return err
	}

	return c.writeResultset(r.Status, r.Resultset)
}

func (c *ClientConn) sortSelectResult(r *mysql.Resultset, stmt *sqlparser.Select) error {
	if stmt.OrderBy == nil {
		return nil
	}

	sk := make([]mysql.SortKey, len(stmt.OrderBy))

	for i, o := range stmt.OrderBy {
		sk[i].Name = sqlparser.String(o.Expr)
		sk[i].Direction = o.Direction
	}

	return r.Sort(sk)
}

func (c *ClientConn) limitSelectResult(r *mysql.Resultset, stmt *sqlparser.Select) error {
	if stmt.Limit == nil {
		return nil
	}

	var offset, count int64
	var err error
	if stmt.Limit.Offset == nil {
		offset = 0
	} else {
		if o, ok := stmt.Limit.Offset.(*sqlparser.SQLVal); !ok || (ok && o.Type != sqlparser.IntVal) {
			return fmt.Errorf("invalid select limit %s", sqlparser.String(stmt.Limit))
		} else {
			if offset, err = strconv.ParseInt(sqlparser.String(o), 10, 64); err != nil {
				return err
			}
		}
	}

	if o, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); !ok || (ok && o.Type != sqlparser.IntVal) {
		return fmt.Errorf("invalid select limit %s", sqlparser.String(stmt.Limit))
	} else {
		if count, err = strconv.ParseInt(sqlparser.String(o), 10, 64); err != nil {
			return err
		} else if count < 0 {
			return fmt.Errorf("invalid select limit %s", sqlparser.String(stmt.Limit))
		}
	}
	if offset > int64(len(r.Values)) {
		r.Values = nil
		r.RowDatas = nil
		return nil
	}

	if offset+count > int64(len(r.Values)) {
		count = int64(len(r.Values)) - offset
	}

	r.Values = r.Values[offset : offset+count]
	r.RowDatas = r.RowDatas[offset : offset+count]

	return nil
}

func (c *ClientConn) buildFuncExprResult(stmt *sqlparser.Select,
	rs []*mysql.Result, funcExprList map[int]string) (*mysql.Resultset, error) {

	var names []string
	var err error
	r := rs[0].Resultset
	funcExprValues := make(map[int]interface{})

	for index, funcName := range funcExprList {
		funcExprValue, err := c.calFuncExprValue(
			funcName,
			rs,
			index,
		)
		if err != nil {
			return nil, err
		}
		funcExprValues[index] = funcExprValue
	}

	r.Values, err = c.buildFuncExprValues(rs, funcExprValues)

	if 0 < len(r.Values) {
		for _, field := range rs[0].Fields {
			names = append(names, string(field.Name))
		}
		r, err = c.buildResultset(rs[0].Fields, names, r.Values)
		if err != nil {
			return nil, err
		}
	} else {
		r = c.newEmptyResultset(stmt)
	}

	return r, nil
}

func (c *ClientConn) buildResultset(fields []*mysql.Field, names []string, values [][]interface{}) (*mysql.Resultset, error) {
	var ExistFields bool
	r := new(mysql.Resultset)

	r.Fields = make([]*mysql.Field, len(names))
	r.FieldNames = make(map[string]int, len(names))

	//use the field def that get from true database
	if len(fields) != 0 {
		if len(r.Fields) == len(fields) {
			ExistFields = true
		} else {
			return nil, my_errors.ErrInvalidArgument
		}
	}

	var b []byte
	var err error

	for i, vs := range values {
		if len(vs) != len(r.Fields) {
			return nil, fmt.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}

		var row []byte
		for j, value := range vs {
			//列的定义
			if i == 0 {
				if ExistFields {
					r.Fields[j] = fields[j]
					r.FieldNames[string(r.Fields[j].Name)] = j
				} else {
					field := &mysql.Field{}
					r.Fields[j] = field
					r.FieldNames[string(r.Fields[j].Name)] = j
					field.Name = hack.Slice(names[j])
					if err = formatField(field, value); err != nil {
						return nil, err
					}
				}

			}
			b, err = formatValue(value)
			if err != nil {
				return nil, err
			}

			row = append(row, mysql.PutLengthEncodedString(b)...)
		}

		r.RowDatas = append(r.RowDatas, row)
	}
	//assign the values to the result
	r.Values = values

	return r, nil
}

//build values of resultset,only build one row
func (c *ClientConn) buildFuncExprValues(rs []*mysql.Result,
	funcExprValues map[int]interface{}) ([][]interface{}, error) {
	values := make([][]interface{}, 0, 1)
	//build a row in one result
	for i := range rs {
		for j := range rs[i].Values {
			for k := range funcExprValues {
				rs[i].Values[j][k] = funcExprValues[k]
			}
			values = append(values, rs[i].Values[j])
			if len(values) == 1 {
				break
			}
		}
		break
	}

	//generate one row just for sum or count
	if len(values) == 0 {
		value := make([]interface{}, len(rs[0].Fields))
		for k := range funcExprValues {
			value[k] = funcExprValues[k]
		}
		values = append(values, value)
	}

	return values, nil
}

func (c *ClientConn) newEmptyResultset(stmt *sqlparser.Select) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(stmt.SelectExprs))

	for i, expr := range stmt.SelectExprs {
		r.Fields[i] = &mysql.Field{}
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			r.Fields[i].Name = []byte("*")
		case *sqlparser.AliasedExpr:
			if !e.As.IsEmpty() {
				r.Fields[i].Name = hack.Slice(sqlparser.String(e.As))
				r.Fields[i].OrgName = hack.Slice(sqlparser.String(e.Expr))
			} else {
				r.Fields[i].Name = hack.Slice(sqlparser.String(e.Expr))
			}
		default:
			r.Fields[i].Name = hack.Slice(sqlparser.String(e))
		}
	}

	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}

//get the index of funcExpr, the value is function name
func (c *ClientConn) getFuncExprs(stmt *sqlparser.Select) map[int]string {
	var f *sqlparser.FuncExpr
	funcExprs := make(map[int]string)

	for i, expr := range stmt.SelectExprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}

		f, ok = aliasedExpr.Expr.(*sqlparser.FuncExpr)
		if !ok {
			continue
		} else {
			f = aliasedExpr.Expr.(*sqlparser.FuncExpr)
			funcName := strings.ToLower(sqlparser.String(f.Name))
			switch funcNameMap[funcName] {
			case FUNC_EXIST:
				funcExprs[i] = funcName
			}
		}
	}
	return funcExprs
}

func (c *ClientConn) getSumFuncExprValue(rs []*mysql.Result,
	index int) (interface{}, error) {
	var sumf float64
	var sumi int64
	var IsInt bool
	var err error
	var result interface{}

	for _, r := range rs {
		for k := range r.Values {
			result, err = r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}

			switch v := result.(type) {
			case int:
				sumi = sumi + int64(v)
				IsInt = true
			case int32:
				sumi = sumi + int64(v)
				IsInt = true
			case int64:
				sumi = sumi + v
				IsInt = true
			case float32:
				sumf = sumf + float64(v)
			case float64:
				sumf = sumf + v
			case []byte:
				tmp, err := strconv.ParseFloat(string(v), 64)
				if err != nil {
					return nil, err
				}

				sumf = sumf + tmp
			default:
				return nil, my_errors.ErrSumColumnType
			}
		}
	}
	if IsInt {
		return sumi, nil
	} else {
		return sumf, nil
	}
}

func (c *ClientConn) getMaxFuncExprValue(rs []*mysql.Result,
	index int) (interface{}, error) {
	var max interface{}
	var findNotNull bool
	if len(rs) == 0 {
		return nil, nil
	} else {
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					max = result
					findNotNull = true
					break
				}
			}
			if findNotNull {
				break
			}
		}
	}
	for _, r := range rs {
		for k := range r.Values {
			result, err := r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}
			switch result.(type) {
			case int64:
				if max.(int64) < result.(int64) {
					max = result
				}
			case uint64:
				if max.(uint64) < result.(uint64) {
					max = result
				}
			case float64:
				if max.(float64) < result.(float64) {
					max = result
				}
			case string:
				if max.(string) < result.(string) {
					max = result
				}
			}
		}
	}
	return max, nil
}

func (c *ClientConn) getMinFuncExprValue(
	rs []*mysql.Result, index int) (interface{}, error) {
	var min interface{}
	var findNotNull bool
	if len(rs) == 0 {
		return nil, nil
	} else {
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					min = result
					findNotNull = true
					break
				}
			}
			if findNotNull {
				break
			}
		}
	}
	for _, r := range rs {
		for k := range r.Values {
			result, err := r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}
			switch result.(type) {
			case int64:
				if min.(int64) > result.(int64) {
					min = result
				}
			case uint64:
				if min.(uint64) > result.(uint64) {
					min = result
				}
			case float64:
				if min.(float64) > result.(float64) {
					min = result
				}
			case string:
				if min.(string) > result.(string) {
					min = result
				}
			}
		}
	}
	return min, nil
}

//calculate  the value funcExpr(sum or count)
func (c *ClientConn) calFuncExprValue(funcName string,
	rs []*mysql.Result, index int) (interface{}, error) {

	var num int64
	switch strings.ToLower(funcName) {
	case CountFunc:
		if len(rs) == 0 {
			return nil, nil
		}
		for _, r := range rs {
			if r != nil {
				for k := range r.Values {
					result, err := r.GetInt(k, index)
					if err != nil {
						return nil, err
					}
					num += result
				}
			}
		}
		return num, nil
	case SumFunc:
		return c.getSumFuncExprValue(rs, index)
	case MaxFunc:
		return c.getMaxFuncExprValue(rs, index)
	case MinFunc:
		return c.getMinFuncExprValue(rs, index)
	case LastInsertIdFunc:
		return c.lastInsertId, nil
	default:
		if len(rs) == 0 {
			return nil, nil
		}
		//get a non-null value of funcExpr
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					return result, nil
				}
			}
		}
	}

	return nil, nil
}

//build select result without group by opt
func (c *ClientConn) buildSelectOnlyResult(rs []*mysql.Result,
	stmt *sqlparser.Select) (*mysql.Result, error) {
	var err error
	r := rs[0].Resultset
	status := c.status | rs[0].Status

	funcExprs := c.getFuncExprs(stmt)
	if len(funcExprs) == 0 {
		for i := 1; i < len(rs); i++ {
			status |= rs[i].Status
			for j := range rs[i].Values {
				r.Values = append(r.Values, rs[i].Values[j])
				r.RowDatas = append(r.RowDatas, rs[i].RowDatas[j])
			}
		}
	} else {
		//result only one row, status doesn't need set
		r, err = c.buildFuncExprResult(stmt, rs, funcExprs)
		if err != nil {
			return nil, err
		}
	}
	return &mysql.Result{
		Status:    status,
		Resultset: r,
	}, nil
}

//build select result with group by opt
func (c *ClientConn) buildSelectGroupByResult(rs []*mysql.Result,
	stmt *sqlparser.Select) (*mysql.Result, error) {
	var err error
	var r *mysql.Result
	var groupByIndexs []int

	fieldLen := len(rs[0].Fields)
	startIndex := fieldLen - len(stmt.GroupBy)
	for startIndex < fieldLen {
		groupByIndexs = append(groupByIndexs, startIndex)
		startIndex++
	}

	funcExprs := c.getFuncExprs(stmt)
	if len(funcExprs) == 0 {
		r, err = c.mergeGroupByWithoutFunc(rs, groupByIndexs)
	} else {
		r, err = c.mergeGroupByWithFunc(rs, groupByIndexs, funcExprs)
	}
	if err != nil {
		return nil, err
	}

	//build result
	names := make([]string, 0, 2)
	if 0 < len(r.Values) {
		r.Fields = r.Fields[:groupByIndexs[0]]
		for i := 0; i < len(r.Fields) && i < groupByIndexs[0]; i++ {
			names = append(names, string(r.Fields[i].Name))
		}
		//delete group by columns in Values
		for i := 0; i < len(r.Values); i++ {
			r.Values[i] = r.Values[i][:groupByIndexs[0]]
		}
		r.Resultset, err = c.buildResultset(r.Fields, names, r.Values)
		if err != nil {
			return nil, err
		}
	} else {
		r.Resultset = c.newEmptyResultset(stmt)
	}

	return r, nil
}

//only merge result without aggregate function in group by opt
func (c *ClientConn) mergeGroupByWithoutFunc(rs []*mysql.Result,
	groupByIndexs []int) (*mysql.Result, error) {
	r := rs[0]
	//load rs into a map
	resultMap, err := c.loadResultIntoMap(rs, groupByIndexs)
	if err != nil {
		return nil, err
	}

	//set status
	status := c.status
	for i := 0; i < len(rs); i++ {
		status = status | rs[i].Status
	}

	//load map into Resultset
	r.Values = nil
	r.RowDatas = nil
	for _, v := range resultMap {
		r.Values = append(r.Values, v.Value)
		r.RowDatas = append(r.RowDatas, v.RowData)
	}
	r.Status = status

	return r, nil
}

type ResultRow struct {
	Value   []interface{}
	RowData mysql.RowData
}

func (c *ClientConn) loadResultIntoMap(rs []*mysql.Result,
	groupByIndexs []int) (map[string]*ResultRow, error) {
	//load Result into map
	resultMap := make(map[string]*ResultRow)
	for _, r := range rs {
		for i := 0; i < len(r.Values); i++ {
			keySlice := r.Values[i][groupByIndexs[0]:]
			mk, err := c.generateMapKey(keySlice)
			if err != nil {
				return nil, err
			}

			resultMap[mk] = &ResultRow{
				Value:   r.Values[i],
				RowData: r.RowDatas[i],
			}
		}
	}

	return resultMap, nil
}

func (c *ClientConn) generateMapKey(groupColumns []interface{}) (string, error) {
	bk := make([]byte, 0, 8)
	separatorBuf, err := formatValue("+")
	if err != nil {
		return "", err
	}

	for _, v := range groupColumns {
		b, err := formatValue(v)
		if err != nil {
			return "", err
		}
		bk = append(bk, b...)
		bk = append(bk, separatorBuf...)
	}

	return string(bk), nil
}

//only merge result with aggregate function in group by opt
func (c *ClientConn) mergeGroupByWithFunc(rs []*mysql.Result, groupByIndexs []int,
	funcExprs map[int]string) (*mysql.Result, error) {
	r := rs[0]
	//load rs into a map, in order to make group
	resultMap, err := c.loadResultWithFuncIntoMap(rs, groupByIndexs, funcExprs)
	if err != nil {
		return nil, err
	}

	//set status
	status := c.status
	for i := 0; i < len(rs); i++ {
		status = status | rs[i].Status
	}

	//change map into Resultset
	r.Values = nil
	r.RowDatas = nil
	for _, v := range resultMap {
		r.Values = append(r.Values, v.Value)
		r.RowDatas = append(r.RowDatas, v.RowData)
	}
	r.Status = status

	return r, nil
}

func (c *ClientConn) loadResultWithFuncIntoMap(rs []*mysql.Result,
	groupByIndexs []int, funcExprs map[int]string) (map[string]*ResultRow, error) {

	resultMap := make(map[string]*ResultRow)
	rt := new(mysql.Result)
	rt.Resultset = new(mysql.Resultset)
	rt.Fields = rs[0].Fields

	//change Result into map
	for _, r := range rs {
		for i := 0; i < len(r.Values); i++ {
			keySlice := r.Values[i][groupByIndexs[0]:]
			mk, err := c.generateMapKey(keySlice)
			if err != nil {
				return nil, err
			}

			if v, ok := resultMap[mk]; ok {
				//init rt
				rt.Values = nil
				rt.RowDatas = nil

				//append v and r into rt, and calculate the function value
				rt.Values = append(rt.Values, r.Values[i], v.Value)
				rt.RowDatas = append(rt.RowDatas, r.RowDatas[i], v.RowData)
				resultTmp := []*mysql.Result{rt}

				for funcIndex, funcName := range funcExprs {
					funcValue, err := c.calFuncExprValue(funcName, resultTmp, funcIndex)
					if err != nil {
						return nil, err
					}
					//set the function value in group by
					resultMap[mk].Value[funcIndex] = funcValue
				}
			} else { //key is not exist
				resultMap[mk] = &ResultRow{
					Value:   r.Values[i],
					RowData: r.RowDatas[i],
				}
			}
		}
	}

	return resultMap, nil
}

func (c *ClientConn) handleSelect(stmt *sqlparser.Select, args []interface{}) error {

	//c.schema
	return nil
}
