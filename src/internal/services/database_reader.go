package services

import "errors"

var (
	ErrConstraintNotImplement = errors.New("constraint Type not implemented")
)

// DatabaseReader is the main interface component to read any type of database.
//
// This service is used by the usecases to export the schema definitions and data in an agnostic database system.
type DatabaseReader interface {
	ListSchemasDefinition() ([]Schema, error)
	GetSchemaDataBatch(schema Schema, batchSize uint, batchCursor BatchCursor) ([]SchemaData, BatchCursor, error)
}

// Schema is an interface that represents the definition of a scheme/table in a database.
type Schema interface{}

// SchemaData is an interface that represents a single data/row inside a scheme/table.
type SchemaData interface{}

// BatchCursor is an interface that represents a Cursor to make batched queries to a database scheme.
type BatchCursor interface{}
