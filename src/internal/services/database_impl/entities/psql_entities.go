package entities

// PSQLDBExtraInfo is the struct used to know all extra information about the PostgreSQL database.
type PSQLDBExtraInfo struct {
	Sequences []PSQLSequence
	Routines  []PSQLRoutine
}

// PSQLSequence is the struct used to represent all data relative to a PostgreSQL sequence.
type PSQLSequence struct {
	SequenceName string
	DataType     string
	StartValue   uint
	MinimumValue uint
	MaximumValue uint
	Increment    uint
	CycleOption  bool
	LastValue    uint
	IsCalled     bool
}

// PSQLArgument is the struct used to represent arguments in routines.
type PSQLArgument struct {
	Name  string
	Type  string
	IsOut bool
}

// RoutineType is a enumeration of types of routines in PostgreSQL.
type RoutineType string

const (
	RoutineTypeFunction  RoutineType = "f"
	RoutineTypeProcedure RoutineType = "p"
)

// PSQLRoutine is the struct used to represent all data relative to a PostgreSQL routine.
type PSQLRoutine struct {
	RoutineType RoutineType
	RoutineName string
	Arguments   []PSQLArgument
	ReturnType  string
	Language    string
	Definition  string
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
