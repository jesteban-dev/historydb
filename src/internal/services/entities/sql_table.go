package entities

import (
	"crypto/sha256"
	"encoding/json"
)

type SchemaType string

const (
	Relational SchemaType = "RELATIONAL"
)

type SQLTable struct {
	SchemaType  SchemaType           `json:"schemaType"`
	TableName   string               `json:"tableName"`
	Columns     []SQLTableColumn     `json:"columns"`
	Constraints []SQLTableConstraint `json:"constraints"`
	ForeignKeys []SQLTableForeignKey `json:"foreignKeys"`
	Indexes     []SQLTableIndex      `json:"indexes"`
}

func (table *SQLTable) GetName() string {
	return table.TableName
}

func (table *SQLTable) Hash() ([32]byte, error) {
	json, err := json.Marshal(table)
	if err != nil {
		return [32]byte{}, err
	}

	hashValue := sha256.Sum256(json)
	return hashValue, nil
}

type SQLTableColumn struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	IsNullable   bool    `json:"isNullable"`
	DefaultValue *string `json:"defaultValue"`
	Position     int     `json:"position"`
}

type ConstraintType string

const (
	PrimaryKey ConstraintType = "PRIMARY KEY"
	Unique     ConstraintType = "UNIQUE"
	Check      ConstraintType = "CHECK"
)

type SQLTableConstraint struct {
	Type       ConstraintType `json:"type"`
	Name       string         `json:"name"`
	Columns    []string       `json:"columns"`
	Definition *string        `json:"definition"`
}

type ActionType string

const (
	NoAction   ActionType = "NO ACTION"
	Restrict   ActionType = "RESTRICT"
	Cascade    ActionType = "CASCADE"
	SetNull    ActionType = "SET NULL"
	SetDefault ActionType = "SET DEFAULT"
)

type SQLTableForeignKey struct {
	Name              string     `json:"name"`
	Columns           []string   `json:"columns"`
	ReferencedTable   string     `json:"referencedTable"`
	ReferencedColumns []string   `json:"referencedColumn"`
	UpdateAction      ActionType `json:"updateAction"`
	DeleteAction      ActionType `json:"deleteAction"`
}

type SQLTableIndex struct {
	Name    string                 `json:"name"`
	Type    string                 `json:"type"`
	Columns []string               `json:"columns"`
	Options map[string]interface{} `json:"options"`
}
