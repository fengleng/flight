package common

const (
	Version = "flight0.1"
	//VersionQueryStr = "select @@version_comment"
	Select         = "select"
	Show           = "Show"
	VersionComment = "@@version_comment"
	DefaultDecimal = 31
	ServerVersion  = "1.0.1-flight"
)

var (
	TK_ID_INSERT   = 1
	TK_ID_UPDATE   = 2
	TK_ID_DELETE   = 3
	TK_ID_REPLACE  = 4
	TK_ID_SET      = 5
	TK_ID_BEGIN    = 6
	TK_ID_COMMIT   = 7
	TK_ID_ROLLBACK = 8
	TK_ID_ADMIN    = 9
	TK_ID_USE      = 10

	TK_ID_SELECT      = 11
	TK_ID_START       = 12
	TK_ID_TRANSACTION = 13
	TK_ID_SHOW        = 14
	TK_ID_TRUNCATE    = 15

	PARSE_TOKEN_MAP = map[string]int{
		"insert":      TK_ID_INSERT,
		"update":      TK_ID_UPDATE,
		"delete":      TK_ID_DELETE,
		"replace":     TK_ID_REPLACE,
		"set":         TK_ID_SET,
		"begin":       TK_ID_BEGIN,
		"commit":      TK_ID_COMMIT,
		"rollback":    TK_ID_ROLLBACK,
		"admin":       TK_ID_ADMIN,
		"select":      TK_ID_SELECT,
		"use":         TK_ID_USE,
		"start":       TK_ID_START,
		"transaction": TK_ID_TRANSACTION,
		"show":        TK_ID_SHOW,
		"truncate":    TK_ID_TRUNCATE,
	}
	// '*'
	COMMENT_PREFIX uint8 = 42
	COMMENT_STRING       = "*"

	//
	TK_STR_SELECT = "select"
	TK_STR_FROM   = "from"
	TK_STR_INTO   = "into"
	TK_STR_SET    = "set"

	TK_STR_TRANSACTION    = "transaction"
	TK_STR_LAST_INSERT_ID = "last_insert_id()"
	TK_STR_MASTER_HINT    = "*master*"
	//show
	TK_STR_COLUMNS         = "columns"
	TK_STR_FIELDS          = "fields"
	TK_STR_VERSION_COMMENT = "@@version_comment"

	SET_KEY_WORDS = map[string]struct{}{
		"names": struct{}{},

		"character_set_results":           struct{}{},
		"@@character_set_results":         struct{}{},
		"@@session.character_set_results": struct{}{},

		"character_set_client":           struct{}{},
		"@@character_set_client":         struct{}{},
		"@@session.character_set_client": struct{}{},

		"character_set_connection":           struct{}{},
		"@@character_set_connection":         struct{}{},
		"@@session.character_set_connection": struct{}{},

		"autocommit":           struct{}{},
		"@@autocommit":         struct{}{},
		"@@session.autocommit": struct{}{},
	}

	TK_STR_DATABASES = "databases"
	TK_STR_TABLES    = "tables"
)
