package entities

// PSQLColumn is the struct used to retrieve the columns into a PostgreSQL table.
type PSQLColumn struct {
	ColumnName             string
	DataType               string
	IsNullable             bool
	ColumnDefault          *string
	OrdinalPosition        uint
	CharacterMaximumLength *uint
}

// PSQLConstraint is the struct used to retrieve the constraints defined in a PostgreSQL table.
type PSQLConstraint struct {
	ConstraintName string
	ConstraintType string
	ColumnName     *string
	Definition     *string
}

// PSQLForeignKey is the struct used to retrieve the foreign keys defined in a PostgreSQL table with its reference.
type PSQLForeignKey struct {
	ConstraintName   string
	ColumnName       string
	ReferencedTable  string
	ReferencedColumn string
	UpdateRule       string
	DeleteRule       string
}

// PSQLIndex is the struct used to retrieve the indexes defined in a PostgreSQL data
type PSQLIndex struct {
	IndexName        string
	IndexType        string
	ColumnNames      []string
	IsUnique         bool
	IsPrimary        bool
	PartialCondition *string
}

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
