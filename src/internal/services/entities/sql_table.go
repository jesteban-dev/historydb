package entities

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"historydb/src/internal/entities"
	"historydb/src/internal/helpers"
	"reflect"
	"sort"
)

type SchemaType string

const (
	Relational SchemaType = "RELATIONAL"
)

type SQLTable struct {
	SchemaType  SchemaType           `json:"schemaType"`
	TableName   string               `json:"tableName"`
	Columns     []SQLTableColumn     `json:"columns,omitempty"`
	Constraints []SQLTableConstraint `json:"constraints,omitempty"`
	ForeignKeys []SQLTableForeignKey `json:"foreignKeys,omitempty"`
	Indexes     []SQLTableIndex      `json:"indexes,omitempty"`
}

func (table *SQLTable) GetName() string {
	return table.TableName
}

func (table *SQLTable) Hash() (string, error) {
	json, err := json.Marshal(table)
	if err != nil {
		return "", err
	}

	hashValue := sha256.Sum256(json)
	return hex.EncodeToString(hashValue[:]), nil
}

func (table *SQLTable) Diff(schema entities.Schema) entities.SchemaDiff {
	diff := SQLTableDiff{
		SchemaType: "RELATIONAL",
	}
	oldTable := schema.(*SQLTable)

	diff.schemaHash, _ = table.Hash()
	diff.PrevRef, _ = schema.Hash()
	diff.AddedColumns, diff.RemovedColumns = helpers.DiffSlices(table.Columns, oldTable.Columns)
	diff.AddedConstraints, diff.RemovedConstraints = helpers.DiffSlices(table.Constraints, oldTable.Constraints)
	diff.AddedForeignKeys, diff.RemovedForeignKeys = helpers.DiffSlices(table.ForeignKeys, oldTable.ForeignKeys)
	diff.AddedIndexes, diff.RemovedIndexes = helpers.DiffSlices(table.Indexes, oldTable.Indexes)

	return diff
}

func (table *SQLTable) ApplyDiff(diff entities.SchemaDiff) entities.Schema {
	updatedTable := *table
	tableDiff := diff.(*SQLTableDiff)

	updatedTable.Columns = make([]SQLTableColumn, 0, len(table.Columns)+len(tableDiff.AddedColumns)-len(tableDiff.RemovedColumns))
outerColumns:
	for _, column := range table.Columns {
		for _, del := range tableDiff.RemovedColumns {
			if column.equal(del) {
				continue outerColumns
			}
		}
		updatedTable.Columns = append(updatedTable.Columns, column)
	}
	updatedTable.Columns = append(updatedTable.Columns, tableDiff.AddedColumns...)
	sort.Slice(updatedTable.Columns, func(i, j int) bool {
		return updatedTable.Columns[i].Position < updatedTable.Columns[j].Position
	})

	updatedTable.Constraints = make([]SQLTableConstraint, 0, len(table.Constraints)+len(tableDiff.AddedConstraints)-len(tableDiff.RemovedConstraints))
outerConstraints:
	for _, constraint := range table.Constraints {
		for _, del := range tableDiff.RemovedConstraints {
			if constraint.equal(del) {
				continue outerConstraints
			}
		}
		updatedTable.Constraints = append(updatedTable.Constraints, constraint)
	}
	updatedTable.Constraints = append(updatedTable.Constraints, tableDiff.AddedConstraints...)
	sort.Slice(updatedTable.Constraints, func(i, j int) bool {
		return updatedTable.Constraints[i].Name < updatedTable.Constraints[j].Name
	})

	updatedTable.ForeignKeys = make([]SQLTableForeignKey, 0, len(table.Constraints)+len(tableDiff.AddedConstraints)-len(tableDiff.RemovedConstraints))
outerFKeys:
	for _, fKey := range table.ForeignKeys {
		for _, del := range tableDiff.RemovedForeignKeys {
			if fKey.equal(del) {
				continue outerFKeys
			}
		}
		updatedTable.ForeignKeys = append(updatedTable.ForeignKeys, fKey)
	}
	updatedTable.ForeignKeys = append(updatedTable.ForeignKeys, tableDiff.AddedForeignKeys...)
	sort.Slice(updatedTable.ForeignKeys, func(i, j int) bool {
		return updatedTable.ForeignKeys[i].Name < updatedTable.ForeignKeys[j].Name
	})

	updatedTable.Indexes = make([]SQLTableIndex, 0, len(table.Constraints)+len(tableDiff.AddedConstraints)-len(tableDiff.RemovedConstraints))
outerIndex:
	for _, idx := range table.Indexes {
		for _, del := range tableDiff.RemovedIndexes {
			if idx.equal(del) {
				continue outerIndex
			}
		}
		updatedTable.Indexes = append(updatedTable.Indexes, idx)
	}
	updatedTable.Indexes = append(updatedTable.Indexes, tableDiff.AddedIndexes...)
	sort.Slice(updatedTable.Indexes, func(i, j int) bool {
		return updatedTable.Indexes[i].Name < updatedTable.Indexes[j].Name
	})

	return &updatedTable
}

type SQLTableDiff struct {
	schemaHash         string               `json:"-"`
	SchemaType         SchemaType           `json:"schemaType"`
	PrevRef            string               `json:"prevRef"`
	AddedColumns       []SQLTableColumn     `json:"addedColumns,omitempty"`
	RemovedColumns     []SQLTableColumn     `json:"removedColumns,omitempty"`
	AddedConstraints   []SQLTableConstraint `json:"addedConstraints,omitempty"`
	RemovedConstraints []SQLTableConstraint `json:"removedConstraints,omitempty"`
	AddedForeignKeys   []SQLTableForeignKey `json:"addedForeignKeys,omitempty"`
	RemovedForeignKeys []SQLTableForeignKey `json:"removedForeignKeys,omitempty"`
	AddedIndexes       []SQLTableIndex      `json:"addedIndexes,omitempty"`
	RemovedIndexes     []SQLTableIndex      `json:"removedIndexes,omitempty"`
}

func (tableDiff SQLTableDiff) GetSchemaHash() string {
	return tableDiff.schemaHash
}

type SQLTableColumn struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	IsNullable   bool    `json:"isNullable"`
	DefaultValue *string `json:"defaultValue"`
	Position     int     `json:"position"`
}

func (c1 SQLTableColumn) equal(c2 SQLTableColumn) bool {
	if (c1.DefaultValue != nil && c2.DefaultValue != nil && *c1.DefaultValue == *c2.DefaultValue) || (c1.DefaultValue == nil && c2.DefaultValue == nil) {
		return c1.Name == c2.Name && c1.Type == c2.Type && c1.IsNullable == c2.IsNullable && c1.Position == c2.Position
	}
	return false
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

func (c1 SQLTableConstraint) equal(c2 SQLTableConstraint) bool {
	if (c1.Definition != nil && c2.Definition != nil && *c1.Definition == *c2.Definition) || (c1.Definition == nil && c2.Definition == nil) {
		if len(c1.Columns) != len(c2.Columns) {
			return false
		}

		for i := 0; i < len(c1.Columns); i++ {
			if c1.Columns[i] != c2.Columns[i] {
				return false
			}
		}
		return c1.Type == c2.Type && c1.Name == c2.Name
	}
	return false
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

func (fk1 SQLTableForeignKey) equal(fk2 SQLTableForeignKey) bool {
	if len(fk1.Columns) != len(fk2.Columns) || len(fk1.ReferencedColumns) != len(fk2.ReferencedColumns) {
		return false
	}
	for i := 0; i < len(fk1.Columns); i++ {
		if fk1.Columns[i] != fk2.Columns[i] {
			return false
		}
	}
	for i := 0; i < len(fk1.ReferencedColumns); i++ {
		if fk1.ReferencedColumns[i] != fk2.ReferencedColumns[i] {
			return false
		}
	}
	return fk1.Name == fk2.Name && fk1.ReferencedTable == fk2.ReferencedTable && fk1.UpdateAction == fk2.UpdateAction && fk1.DeleteAction == fk2.DeleteAction
}

type SQLTableIndex struct {
	Name    string                 `json:"name"`
	Type    string                 `json:"type"`
	Columns []string               `json:"columns"`
	Options map[string]interface{} `json:"options"`
}

func (idx1 SQLTableIndex) equal(idx2 SQLTableIndex) bool {
	if len(idx1.Columns) != len(idx2.Columns) || len(idx1.Options) != len(idx2.Options) {
		return false
	}
	for i := 0; i < len(idx1.Columns); i++ {
		if idx1.Columns[i] != idx2.Columns[i] {
			return false
		}
	}
	for k, v1 := range idx1.Options {
		v2, ok := idx2.Options[k]
		if !ok || !reflect.DeepEqual(v1, v2) {
			return false
		}
	}

	return idx1.Name == idx2.Name && idx1.Type == idx2.Type
}
