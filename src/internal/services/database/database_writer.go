package database_services

import "historydb/src/internal/entities"

// DatabaseWriter is the interface that defines the functionality to insert data into the DB.
//
// BeginTransaction() -> Begins a DB transaction.
// CommitTransaction() -> Commits a DB transaction.
// RollbacksTransaction() -> Rollbacks a DB transaction.
// SaveSchemaDependency() -> Inserts a schema dependency into the DB.
// SaveSchema() -> Inserts a schema into the DB.
// SaveSchemaRules() -> Updates a schema with its rules and constraints in the DB.
// SaveSchemaRecords() -> Inserts a chunk of data into its schema in the DB.
// SaveRoutine() -> Inserts a routine into the DB.
type DatabaseWriter interface {
	BeginTransaction() error
	CommitTransaction() error
	RollbackTransaction() error

	SaveSchemaDependency(dependency entities.SchemaDependency) error
	SaveSchema(schema entities.Schema) error
	SaveSchemaRules(schema entities.Schema) error
	SaveSchemaRecords(schema entities.Schema, chunk entities.SchemaRecordChunk) error
	SaveRoutine(routine entities.Routine) error
}
