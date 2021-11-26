package sqlparser

import (
	bytes2 "bytes"
	"reflect"
	"testing"
)

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func checkEqual(t *testing.T, src interface{}, dst interface{}) {
	if !reflect.DeepEqual(src, dst) {
		t.Fatalf("not equal %v and %v", src, dst)
	}
}

func TestDropTableParsing(t *testing.T) {
	_, err := Parse(`DROP table if exists users`)

	checkErr(t, err)
}

func TestBacktick(t *testing.T) {
	t.Log(Backtick("gg\\'g"))
	t.Log(uint32('`'))
}

func TestSelectParsing(t *testing.T) {
	//sta, err := Parse(`SELECT *,count(id) from users where id = 1 order by created_at limit 1 offset 3`)
	sta2, err := Parse(`select /*gsdfgdfg*/ * from qrfm_daily_static left join clientlog_client_log_task cclt on qrfm_daily_static.app_id = cclt.app_id where qrfm_daily_static.id = cclt.id;`)
	//sta3, err := Parse(`delete from tt where id = 9 ;`)
	sta4, err := Parse("create  database if not exists tt /*gsdfgdfg*/ /*tt:cc */ default character set = 'utfmb4' default collate 'utfmb4_genci';")
	sta5, err := Parse("insert /*tt:cc */ /*gg:vv*/ into  flight_table1(id, name, age) values (2,'ts2',25);")
	//t.Log(sta)
	//t.Log(sta2)
	v := sta4.(*DbDDL)

	c := v.Comments
	for _, bytes := range c {
		//var lastByte byte = 0
		//trimFunc := strings.TrimFunc(string(bytes), func(r rune) bool {
		//	if (lastByte=='/'&&r=='*') || lastByte=='*'&&r=='/'{
		//
		//	}
		//})
		var newBytes bytes2.Buffer
		for i := 0; i < len(bytes); i++ {
			b := bytes[i]
			if b == '/' {
				if i < len(bytes)-1 {
					v := bytes[i]
					if v == '/' || v == '*' {
						i++
						continue
					}
				}
			} else if b == '*' {

			}
			//lastByte = b
			newBytes.WriteByte(b)
		}

		//var newBytes bytes2.Buffer
		//for _, b := range bytes {
		//	if b=='*' || b=='/' {
		//
		//	}
		//	if (lastByte=='/'&&b=='*') || lastByte=='*'&&b=='/'{
		//		continue
		//	}
		//	lastByte = b
		//	newBytes.WriteByte(b)
		//}

		//for _, b := range bytes {
		//
		//}
		t.Log(newBytes.String())
	}
	t.Log(sta5)
	t.Log(sta2)

	for _, comment := range sta5.(*Insert).Comments {
		t.Log(string(comment))
	}
	t.Log(sta4)
	checkErr(t, err)
}

func TestCreateTableParsing(t *testing.T) {
	st, err := Parse(`
		CREATE TABLE cc.users (
		  id bigint(20) unsigned NOT NULL,
		  other_id bigint(20) unsigned NOT NULL,
		  enum_column enum('a','b','c','d') DEFAULT NULL,
		  int_column int(10) DEFAULT '0',
		  PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		`)
	t.Log(st)
	checkErr(t, err)
}

func TestCreateTableWithPartition(t *testing.T) {
	_, err := Parse(`
CREATE TABLE histories (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  user_id bigint(20) unsigned NOT NULL,
  note text NOT NULL,
  created_at datetime NOT NULL,
  updated_at datetime NOT NULL,
  PRIMARY KEY (id,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50500 PARTITION BY RANGE  COLUMNS(created_at)
(PARTITION p201812 VALUES LESS THAN ('2019-01-01') ENGINE = InnoDB,
 PARTITION p201901 VALUES LESS THAN ('2019-02-01') ENGINE = InnoDB,
 PARTITION p201902 VALUES LESS THAN ('2019-03-01') ENGINE = InnoDB,
 PARTITION p201903 VALUES LESS THAN ('2019-04-01') ENGINE = InnoDB) */;
`)
	checkErr(t, err)
}

func TestShowCreateTableParsing(t *testing.T) {
	ast, err := Parse(`SHOW CREATE TABLE users`)
	checkErr(t, err)
	switch stmt := ast.(type) {
	case *Show:
		checkEqual(t, "users", stmt.TableName)
	default:
		t.Fatalf("%+v", "type mismatch")
	}
}
