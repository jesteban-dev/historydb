package entities

import (
	"crypto/sha256"
	"encoding/json"
)

// TableRow is the struct where the table row in SQL will be saved to process it.
type TableRow map[string]interface{}

// TableColumn is the struct where the table column metadata in SQL will be saved to process it.
type TableColumn struct {
	ColumnName     string  `json:"name"`
	ColumnType     string  `json:"type"`
	IsNullable     bool    `json:"isNullable"`
	DefaultValue   *string `json:"defaultValue"`
	ColumnPosition uint    `json:"position"`
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
	ConstraintType ConstraintType `json:"type"`
	ConstraintName string         `json:"name"`
	Columns        []string       `json:"columns"`
	Definition     *string        `json:"definition"`
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
	ConstraintName    string     `json:"name"`
	Columns           []string   `json:"columns"`
	ReferencedTable   string     `json:"referencedTable"`
	ReferencedColumns []string   `json:"referencedColumn"`
	UpdateAction      ActionType `json:"updateAction"`
	DeleteAction      ActionType `json:"deleteAction"`
}

// Index is the struct where the table index metadata in SQL will be saved to process it.
type Index struct {
	IndexName string                 `json:"name"`
	IndexType string                 `json:"type"`
	Columns   []string               `json:"columns"`
	Options   map[string]interface{} `json:"options"`
}

// BatchCursor is the struct used in SQL languages to batch the data queries in a table.
type SQLBatchCursor struct {
	Offset uint
	LastPK interface{}
}

// SQLTable is the struct that represents all the SQL Table metadata.
type SQLTable struct {
	hash        *[32]byte         `json:"-"`
	TableName   string            `json:"name"`
	Columns     []TableColumn     `json:"columns"`
	Constraints []TableConstraint `json:"constraints"`
	ForeignKeys []ForeignKey      `json:"foreignKeys"`
	Indexes     []Index           `json:"indexes"`
}

func (sqlTable *SQLTable) Hash() ([32]byte, error) {
	if sqlTable.hash == nil {
		data, err := json.Marshal(sqlTable)
		if err != nil {
			return [32]byte{}, ErrHashFailed
		}

		hashValue := sha256.Sum256(data)
		sqlTable.hash = &hashValue
	}

	return *sqlTable.hash, nil
}

func (sqlTable *SQLTable) ToJSON() ([]byte, error) {
	data, err := json.MarshalIndent(sqlTable, "", "  ")
	if err != nil {
		return []byte{}, err
	}

	return data, nil
}
