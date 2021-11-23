package client_conn

import (
	"github.com/fengleng/flight/common"
	"github.com/fengleng/flight/server/my_errors"
	"github.com/fengleng/flight/sqlparser/tidbparser/dependency/util/hack"
	"github.com/fengleng/go-mysql-client/mysql"
	"strings"
)

func (c *ClientConn) PreHand(sql string) (r *mysql.Result, isHand bool, isNotWriteResult bool, err error) {

	tokens := strings.FieldsFunc(sql, common.IsSqlSep)
	tokenLen := len(tokens)
	if tokenLen < 1 {
		return nil, false, false, my_errors.ErrCmdUnsupport
	}
	r, isHand, err = &mysql.Result{
		Status: mysql.SERVER_STATUS_AUTOCOMMIT,
		Resultset: &mysql.Resultset{
			FieldNames: map[string]int{},
		},
	}, false, nil

	var tokenId, ok = common.PARSE_TOKEN_MAP[strings.ToLower(tokens[0])]
	if ok {
		switch tokenId {
		case common.TK_ID_SELECT:
			return c.preHandSelect(sql, tokens, tokenLen)
		//case mysql.TK_ID_DELETE:
		//	return c.getDeleteExecDB(sql, tokens, tokensLen)
		//case mysql.TK_ID_INSERT, mysql.TK_ID_REPLACE:
		//	return c.getInsertOrReplaceExecDB(sql, tokens, tokensLen)
		//case mysql.TK_ID_UPDATE:
		//	return c.getUpdateExecDB(sql, tokens, tokensLen)
		//case mysql.TK_ID_SET:
		//	return c.getSetExecDB(sql, tokens, tokensLen)
		case common.TK_ID_SHOW:
			return c.preHandShow(sql, tokens, tokenLen)
		case common.TK_ID_USE:
			if tokenLen == 2 {
				err = c.handleUseDB(tokens[1])
			} else {
				err = mysql.NewDefaultError(mysql.ER_UNKNOWN_ERROR)
			}
			isHand = true
			isNotWriteResult = true
			return
		//case mysql.TK_ID_TRUNCATE:
		//	return c.getTruncateExecDB(sql, tokens, tokensLen)
		default:
			return
		}
	}
	return
}

func (c *ClientConn) preHandSelect(sql string, tokens []string, tokenLen int) (r *mysql.Result, isHand bool, isNotWriteResult bool, err error) {
	r, isHand, err = &mysql.Result{
		Status: mysql.SERVER_STATUS_AUTOCOMMIT,
		Resultset: &mysql.Resultset{
			FieldNames: map[string]int{},
		},
	}, false, nil

	if tokenLen == 2 && tokens[1] == common.TK_STR_VERSION_COMMENT {
		columnName := "@@version_comment"
		r.FieldNames[columnName] = 0
		f := &mysql.Field{
			Name:         hack.Slice(columnName),
			OrgName:      hack.Slice(columnName),
			Charset:      uint16(c.srv.Cfg.CollationId),
			ColumnLength: 112,
			Type:         mysql.MYSQL_TYPE_VAR_STRING,
			Decimal:      common.DefaultDecimal,
		}
		r.Resultset.Fields = append(r.Resultset.Fields, f)

		rd := mysql.RowData(mysql.PutLengthEncodedString(hack.Slice(common.Version)))
		r.Resultset.RowDatas = append(r.RowDatas, rd)
		isHand = true
	}

	return
}

func (c *ClientConn) preHandShow(sql string, tokens []string, tokenLen int) (r *mysql.Result, isHand bool, isNotWriteResult bool, err error) {
	r, isHand, err = &mysql.Result{
		Status: mysql.SERVER_STATUS_AUTOCOMMIT + mysql.SERVER_STATUS_NO_INDEX_USED,
		Resultset: &mysql.Resultset{
			FieldNames: map[string]int{},
		},
	}, false, nil

	if tokenLen == 2 {
		if strings.ToLower(tokens[1]) == common.TK_STR_DATABASES {
			columnName := "Database"
			r.FieldNames[columnName] = 0
			f := &mysql.Field{
				Schema:       hack.Slice("information_schema"),
				Table:        hack.Slice("SCHEMATA"),
				OrgTable:     hack.Slice("SCHEMATA"),
				Name:         hack.Slice(columnName),
				OrgName:      hack.Slice("SCHEMA_NAME"),
				Charset:      uint16(c.srv.Cfg.CollationId),
				ColumnLength: 256,
				Type:         253,
				//Decimal:      ,
				Flag: 1,
			}
			r.Resultset.Fields = append(r.Resultset.Fields, f)
			for schemaName, _ := range c.srv.SchemaMap {
				rd := mysql.RowData(mysql.PutLengthEncodedString(hack.Slice(schemaName)))
				r.Resultset.RowDatas = append(r.RowDatas, rd)
			}
			isHand = true
		} else if tokens[1] == common.TK_STR_TABLES {
			if c.schema == nil {
				err = mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
				return
			}
			isHand = true
			r, err = c.schema.DefaultBackendNode.Master.Execute("SHOW TABLES;")
			return
		} else {
			err = my_errors.ErrCmdUnsupport
			return
		}
	}
	return
}
