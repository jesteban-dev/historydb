package services

import (
	"historydb/src/internal/entities"
)

// DatabaseReader is the interface that defines the components that will read the databases.
//
// ListSchemaNames obtaines all the schema names from the database.
// GetSchemaDefinition obtaines the schema from the database given a schema name.
type DatabaseReader interface {
	CheckDBIsEmpty() (bool, error)
	ListSchemaNames() ([]string, error)
	ListSchemaDependencies() ([]entities.SchemaDependency, error)
	GetSchemaDefinition(schemaName string) (entities.Schema, error)
	GetSchemaDataLength(schemaName string) (int, error)
	GetSchemaDataBatchAndChunkSize(schemaName string) (int, int, error)
	GetSchemaDataChunk(schema entities.Schema, chunkSize int, chunkCursor entities.ChunkCursor) (entities.SchemaDataChunk, entities.ChunkCursor, error)
}
