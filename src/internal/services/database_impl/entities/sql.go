package entities

// SQLTable is the struct that represents all the SQL Table metadata.
type SQLTable struct {
	TableName   string
	Columns     []TableColumn
	Constraints []TableConstraint
	ForeignKeys []ForeignKey
	Indexes     []Index
}

// TableRow is the struct where the table row in SQL will be saved to process it.
type TableRow map[string]interface{}

// TableColumn is the struct where the table column metadata in SQL will be saved to process it.
type TableColumn struct {
	ColumnName     string
	ColumnType     string
	IsNullable     bool
	DefaultValue   *string
	ColumnPosition uint
}

// ConstraintType is an enumeration of the supported constraint types in SQL.
type ConstraintType string

const (
	PrimaryKey ConstraintType = "PRIMARY KEY"
	Unique     ConstraintType = "UNIQUE"
	Check      ConstraintType = "CHECK"
)

// TableConstraint is the struct where the table constraint metadata in SQL will be saved to process it.
type TableConstraint struct {
	ConstraintType ConstraintType
	ConstraintName string
	Columns        []string
	Definition     *string
}

// ActionType is an enumeration of the supported update and delete action for foreign keys in SQL.
type ActionType string

const (
	NoAction   ActionType = "NO ACTION"
	Restrict   ActionType = "RESTRICT"
	Cascade    ActionType = "CASCADE"
	SetNull    ActionType = "SET NULL"
	SetDefault ActionType = "SET DEFAULT"
)

// ForeignKey is the struct where the table foreign key metadata in SQL will be saved to process it.
type ForeignKey struct {
	ConstraintName    string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	UpdateAction      ActionType
	DeleteAction      ActionType
}

// Index is the struct where the table index metadata in SQL will be saved to process it.
type Index struct {
	IndexName string
	IndexType string
	Columns   []string
	Options   map[string]interface{}
}

// BatchCursor is the struct used in SQL languages to batch the data queries in a table.
type BatchCursor struct {
	Offset uint
	LastPK interface{}
}
