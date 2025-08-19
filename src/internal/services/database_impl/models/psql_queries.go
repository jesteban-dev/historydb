package models

// PSQLColumn is the struct used to retrieve the columns into a PostgreSQL table.
type PSQLColumnQuery struct {
	ColumnName             string
	DataType               string
	IsNullable             bool
	ColumnDefault          *string
	OrdinalPosition        uint
	CharacterMaximumLength *uint
	NumericPrecision       *uint
	NumericScale           *uint
}

// PSQLConstraint is the struct used to retrieve the constraints defined in a PostgreSQL table.
type PSQLConstraintQuery struct {
	ConstraintName string
	ConstraintType string
	ColumnName     *string
	Definition     *string
}

// PSQLForeignKey is the struct used to retrieve the foreign keys defined in a PostgreSQL table with its reference.
type PSQLForeignKeyQuery struct {
	ConstraintName   string
	ColumnName       string
	ReferencedSchema string
	ReferencedTable  string
	ReferencedColumn string
	UpdateRule       string
	DeleteRule       string
}

// PSQLIndex is the struct used to retrieve the indexes defined in a PostgreSQL database.
type PSQLIndexQuery struct {
	IndexName        string
	IndexType        string
	ColumnNames      []string
	IsUnique         bool
	PartialCondition *string
}

// PSQLSequenceDataQuery is the struct used to retrieve the sequences metadata in a PostgreSQL database.
type PSQLSequenceDataQuery struct {
	SequenceSchema string
	SequenceName   string
	DataType       string
	StartValue     uint
	MinimumValue   uint
	MaximumValue   uint
	Increment      uint
	CycleOption    bool
}

// PSQLRoutineQuery is the struct used to retrieve the routine metadata in a PostgreSQL database.
type PSQLRoutineQuery struct {
	SchemaName   string
	FunctionName string
	Type         string
	Arguments    string
	ReturnType   string
	Language     string
	Definition   string
}
