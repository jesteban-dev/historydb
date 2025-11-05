package services

import "historydb/src/internal/entities"

// DatabaseWriter is the interface that defines the components that will write into the databases.
//
// WriteSchema will write the schema definition into the database.
type DatabaseWriter interface {
	BeginTransaction() error
	WriteSchemaDependency(dependency entities.SchemaDependency) error
	WriteSchema(schema entities.Schema) error
	WriteSchemaRules(schema entities.Schema) error
	CommitTransaction() error
	RollbackTransaction() error
}
