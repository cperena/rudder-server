package warehouseutils

var ReservedKeywords = map[string]map[string]bool{
	"SNOWFLAKE": {
		"ACCOUNT":           true,
		"ALL":               true,
		"ALTER":             true,
		"AND":               true,
		"ANY":               true,
		"AS":                true,
		"BETWEEN":           true,
		"BY":                true,
		"CASE":              true,
		"CAST":              true,
		"CHECK":             true,
		"COLUMN":            true,
		"CONNECT":           true,
		"CONNECTION":        true,
		"CONSTRAINT":        true,
		"CREATE":            true,
		"CROSS":             true,
		"CURRENT":           true,
		"CURRENT_DATE":      true,
		"CURRENT_TIME":      true,
		"CURRENT_TIMESTAMP": true,
		"CURRENT_USER":      true,
		"DATABASE":          true,
		"DELETE":            true,
		"DISTINCT":          true,
		"DROP":              true,
		"ELSE":              true,
		"EXISTS":            true,
		"FALSE":             true,
		"FOLLOWING":         true,
		"FOR":               true,
		"FROM":              true,
		"FULL":              true,
		"GRANT":             true,
		"GROUP":             true,
		"GSCLUSTER":         true,
		"HAVING":            true,
		"ILIKE":             true,
		"IN":                true,
		"INCREMENT":         true,
		"INNER":             true,
		"INSERT":            true,
		"INTERSECT":         true,
		"INTO":              true,
		"IS":                true,
		"ISSUE":             true,
		"JOIN":              true,
		"LATERAL":           true,
		"LEFT":              true,
		"LIKE":              true,
		"LOCALTIME":         true,
		"LOCALTIMESTAMP":    true,
		"MINUS":             true,
		"NATURAL":           true,
		"NOT":               true,
		"NULL":              true,
		"OF":                true,
		"ON":                true,
		"OR":                true,
		"ORDER":             true,
		"ORGANIZATION":      true,
		"QUALIFY":           true,
		"REGEXP":            true,
		"REVOKE":            true,
		"RIGHT":             true,
		"RLIKE":             true,
		"ROW":               true,
		"ROWS":              true,
		"SAMPLE":            true,
		"SCHEMA":            true,
		"SELECT":            true,
		"SET":               true,
		"SOME":              true,
		"START":             true,
		"TABLE":             true,
		"TABLESAMPLE":       true,
		"THEN":              true,
		"TO":                true,
		"TRIGGER":           true,
		"TRUE":              true,
		"TRY_CAST":          true,
		"UNION":             true,
		"UNIQUE":            true,
		"UPDATE":            true,
		"USING":             true,
		"VALUES":            true,
		"VIEW":              true,
		"WHEN":              true,
		"WHENEVER":          true,
		"WHERE":             true,
		"WITH":              true,
	},
	"RS": {
		"AES128":            true,
		"AES256":            true,
		"ALL":               true,
		"ALLOWOVERWRITE":    true,
		"ANALYSE":           true,
		"ANALYZE":           true,
		"AND":               true,
		"ANY":               true,
		"ARRAY":             true,
		"AS":                true,
		"ASC":               true,
		"AUTHORIZATION":     true,
		"AZ64":              true,
		"BACKUP":            true,
		"BETWEEN":           true,
		"BINARY":            true,
		"BLANKSASNULL":      true,
		"BOTH":              true,
		"BYTEDICT":          true,
		"BZIP2":             true,
		"CASE":              true,
		"CAST":              true,
		"CHECK":             true,
		"COLLATE":           true,
		"COLUMN":            true,
		"CONSTRAINT":        true,
		"CREATE":            true,
		"CREDENTIALS":       true,
		"CROSS":             true,
		"CURRENT_DATE":      true,
		"CURRENT_TIME":      true,
		"CURRENT_TIMESTAMP": true,
		"CURRENT_USER":      true,
		"CURRENT_USER_ID":   true,
		"DEFAULT":           true,
		"DEFERRABLE":        true,
		"DEFLATE":           true,
		"DEFRAG":            true,
		"DELTA":             true,
		"DELTA32K":          true,
		"DESC":              true,
		"DISABLE":           true,
		"DISTINCT":          true,
		"DO":                true,
		"ELSE":              true,
		"EMPTYASNULL":       true,
		"ENABLE":            true,
		"ENCODE":            true,
		"ENCRYPT     ":      true,
		"ENCRYPTION":        true,
		"END":               true,
		"EXCEPT":            true,
		"EXPLICIT":          true,
		"FALSE":             true,
		"FOR":               true,
		"FOREIGN":           true,
		"FREEZE":            true,
		"FROM":              true,
		"FULL":              true,
		"GLOBALDICT256":     true,
		"GLOBALDICT64K":     true,
		"GRANT":             true,
		"GROUP":             true,
		"GZIP":              true,
		"HAVING":            true,
		"IDENTITY":          true,
		"IGNORE":            true,
		"ILIKE":             true,
		"IN":                true,
		"INITIALLY":         true,
		"INNER":             true,
		"INTERSECT":         true,
		"INTO":              true,
		"IS":                true,
		"ISNULL":            true,
		"JOIN":              true,
		"LANGUAGE":          true,
		"LEADING":           true,
		"LEFT":              true,
		"LIKE":              true,
		"LIMIT":             true,
		"LOCALTIME":         true,
		"LOCALTIMESTAMP":    true,
		"LUN":               true,
		"LUNS":              true,
		"LZO":               true,
		"LZOP":              true,
		"MINUS":             true,
		"MOSTLY13":          true,
		"MOSTLY32":          true,
		"MOSTLY8":           true,
		"NATURAL":           true,
		"NEW":               true,
		"NOT":               true,
		"NOTNULL":           true,
		"NULL":              true,
		"NULLS":             true,
		"OFF":               true,
		"OFFLINE":           true,
		"OFFSET":            true,
		"OID":               true,
		"OLD":               true,
		"ON":                true,
		"ONLY":              true,
		"OPEN":              true,
		"OR":                true,
		"ORDER":             true,
		"OUTER":             true,
		"OVERLAPS":          true,
		"PARALLEL":          true,
		"PARTITION":         true,
		"PERCENT":           true,
		"PERMISSIONS":       true,
		"PLACING":           true,
		"PRIMARY":           true,
		"RAW":               true,
		"READRATIO":         true,
		"RECOVER":           true,
		"REFERENCES":        true,
		"RESPECT":           true,
		"REJECTLOG":         true,
		"RESORT":            true,
		"RESTORE":           true,
		"RIGHT":             true,
		"SELECT":            true,
		"SESSION_USER":      true,
		"SIMILAR":           true,
		"SNAPSHOT ":         true,
		"SOME":              true,
		"SYSDATE":           true,
		"SYSTEM":            true,
		"TABLE":             true,
		"TAG":               true,
		"TDES":              true,
		"TEXT255":           true,
		"TEXT32K":           true,
		"THEN":              true,
		"TIMESTAMP":         true,
		"TO":                true,
		"TOP":               true,
		"TRAILING":          true,
		"TRUE":              true,
		"TRUNCATECOLUMNS":   true,
		"UNION":             true,
		"UNIQUE":            true,
		"USER":              true,
		"USING":             true,
		"VERBOSE":           true,
		"WALLET":            true,
		"WHEN":              true,
		"WHERE":             true,
		"WITH":              true,
		"WITHOUT":           true,
	},
	"BQ": {
		"ALL":                  true,
		"AND":                  true,
		"ANY":                  true,
		"ARRAY":                true,
		"AS":                   true,
		"ASC":                  true,
		"ASSERT_ROWS_MODIFIED": true,
		"AT":                   true,
		"BETWEEN":              true,
		"BY":                   true,
		"CASE":                 true,
		"CAST":                 true,
		"COLLATE":              true,
		"CONTAINS":             true,
		"CREATE":               true,
		"CROSS":                true,
		"CUBE":                 true,
		"CURRENT":              true,
		"DEFAULT":              true,
		"DEFINE":               true,
		"DESC":                 true,
		"DISTINCT":             true,
		"ELSE":                 true,
		"END":                  true,
		"ENUM":                 true,
		"ESCAPE":               true,
		"EXCEPT":               true,
		"EXCLUDE":              true,
		"EXISTS":               true,
		"EXTRACT":              true,
		"FALSE":                true,
		"FETCH":                true,
		"FOLLOWING":            true,
		"FOR":                  true,
		"FROM":                 true,
		"FULL":                 true,
		"GROUP":                true,
		"GROUPING":             true,
		"GROUPS":               true,
		"HASH":                 true,
		"HAVING":               true,
		"IF":                   true,
		"IGNORE":               true,
		"IN":                   true,
		"INNER":                true,
		"INTERSECT":            true,
		"INTERVAL":             true,
		"INTO":                 true,
		"IS":                   true,
		"JOIN":                 true,
		"LATERAL":              true,
		"LEFT":                 true,
		"LIKE":                 true,
		"LIMIT":                true,
		"LOOKUP":               true,
		"MERGE":                true,
		"NATURAL":              true,
		"NEW":                  true,
		"NO":                   true,
		"NOT":                  true,
		"NULL":                 true,
		"NULLS":                true,
		"OF":                   true,
		"ON":                   true,
		"OR":                   true,
		"ORDER":                true,
		"OUTER":                true,
		"OVER":                 true,
		"PARTITION":            true,
		"PRECEDING":            true,
		"PROTO":                true,
		"RANGE":                true,
		"RECURSIVE":            true,
		"RESPECT":              true,
		"RIGHT":                true,
		"ROLLUP":               true,
		"ROWS":                 true,
		"SELECT":               true,
		"SET":                  true,
		"SOME":                 true,
		"STRUCT":               true,
		"TABLESAMPLE":          true,
		"THEN":                 true,
		"TO":                   true,
		"TREAT":                true,
		"TRUE":                 true,
		"UNBOUNDED":            true,
		"UNION":                true,
		"UNNEST":               true,
		"USING":                true,
		"WHEN":                 true,
		"WHERE":                true,
		"WINDOW":               true,
		"WITH":                 true,
		"WITHIN":               true,
	},
}
