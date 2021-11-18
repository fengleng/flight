package sqlparser

import "testing"

func TestBuildParsedQuery(t *testing.T) {
	sql := "select * from tt where id = :v1 and c = :v2 and a = :v3"
	statement, err := Parse(sql)
	if err != nil {
		t.Log(err)
	}
	parsedQuery := GenerateParsedQuery(statement)
	//query := BuildParsedQuery("select * from tt where id = %a", 1)
	t.Log(parsedQuery)
	//vv := make([]byte,0,len(parsedQuery.Query))
	//var min,offset,l =0, 0,0
	//for i := 0; i < len(parsedQuery.Query); {
	//
	//
	//	if len(parsedQuery.bindLocations)>0 {
	//		offset = parsedQuery.bindLocations[0].offset
	//		l = parsedQuery.bindLocations[0].length
	//	}
	//	for i<offset {
	//		i++
	//	}
	//	vv =  append(vv,parsedQuery.Query[min:i]...)
	//	i+=l
	//	min = i
	//	vv = append(vv,byte(1))
	//}
	//var o = 0
	//for i := 0; i < len(parsedQuery.bindLocations); i++ {
	//	vv = append(vv,parsedQuery.Query[o:parsedQuery.bindLocations[i].offset]...)
	//	vv = append(vv,[]byte("11")...)
	//	o = parsedQuery.bindLocations[i].offset+parsedQuery.bindLocations[i].length
	//}
	//s := parsedQuery.Query[:parsedQuery.bindLocations[0].offset]
	//s = append([]byte(s),[]byte["11"]...)
	//t.Log(s)
	//t.Log(string(vv))
	t.Log(len("select * from tt where id = ? and c = ? and a = ? 法国撒旦发生"))
	i := 0
	for _, c := range "select * from tt where id = ? and c = ? and a = ? 法国撒旦发生" {
		t.Log(string(c))
		i++
	}
	t.Log(i)
	//for _, location := range parsedQuery.bindLocations {
	//	for i := location.offset; i < location.offset+location.length; i++ {
	//		parsedQuery.Query[i] = ''
	//	}
	//}
}
