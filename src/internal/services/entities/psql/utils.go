package psql

// ComparablePK is a map used to know which data types in a primary key can be used to retrieve batched data from querying a table.
// This way when batching a table we can compare by primary key instead of OFFSET so it will improve the query performace.
var ComparablePK = map[string]bool{
	"smallint":                    true,
	"integer":                     true,
	"bigint":                      true,
	"decimal":                     true,
	"numeric":                     true,
	"real":                        true,
	"double precision":            true,
	"character":                   true,
	"character varying":           true,
	"text":                        true,
	"uuid":                        true,
	"date":                        true,
	"timestamp without time zone": true,
	"timestamp with time zone":    true,
	"time without time zone":      true,
	"time with time zone":         true,
	"inet":                        true,
	"cidr":                        true,
}
