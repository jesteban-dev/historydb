package services

import (
	"errors"
	"historydb/src/internal/services/entities"
)

var (
	ErrConstraintNotImplement = errors.New("constraint Type not implemented")
)

// DatabaseReader is the main interface component to read any type of database.
//
// This service is used by the usecases to export the schema definitions and data in an agnostic database system.
type DatabaseReader interface {
	ListSchemasDefinition() ([]entities.Schema, error)
	GetDatabaseExtraInfo() (entities.DBExtraInfo, error)
	GetSchemaDataBatch(schema entities.Schema, batchSize uint, batchCursor entities.BatchCursor) ([]entities.SchemaData, entities.BatchCursor, error)
}
