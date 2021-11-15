package common

import "github.com/fengleng/flight/sqlparser/sqlparser"

var SpString = sqlparser.String

func IsSqlSep(r rune) bool {
	return r == ' ' || r == ',' ||
		r == '\t' || r == '/' ||
		r == '\n' || r == '\r'
}
