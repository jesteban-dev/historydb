package database_services

import "historydb/src/internal/entities"

// DatabaseReader is the interface that defines the functionality for querying the DB.
//
// CheckDBIsEmpty() -> Checks the DB is empty. It is used when we desire to save our backup into a DB so we need an empty DB.
// ListSchemaDependencies() -> List the DB dependencies. (Sequences, etc...)
// ListSchemaNames() -> Retrieves all the schema names from the DB.
// GetSchemaDefinition() -> Retrieves the schema definition from a DB given its name. (Tables, etc...)
// GetSchemaRecordMetadata() -> Retrieves the metadata needed to get the records in a single schema. (record size, total records)
// GetSchemaRecordChunk() -> Retrieves a chunk of records from the given schema and use a cursor to iterate over it.
type DatabaseReader interface {
	CheckDBIsEmpty() (bool, error)

	ListSchemaDependencies() ([]entities.SchemaDependency, error)
	ListSchemaNames() ([]string, error)
	GetSchemaDefinition(schemaName string) (entities.Schema, error)
	GetSchemaRecordMetadata(schemaName string) (entities.SchemaRecordMetadata, error)
	GetSchemaRecordChunk(schema entities.Schema, chunkSize int64, chunkCursor interface{}) (entities.SchemaRecordChunk, interface{}, error)
}
