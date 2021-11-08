package client_conn

import (
	"fmt"
	"github.com/fengleng/flight/sqlparser/sqlparser"
	"github.com/fengleng/go-common/core/hack"
	"github.com/fengleng/go-mysql-client/mysql"
	"strconv"
)

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

	c.sortSelectResult(r.Resultset, stmt)
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
