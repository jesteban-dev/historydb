package database_services

import "historydb/src/internal/entities"

// DatabaseWriter is the interface that defines the functionality to insert data into the DB.
//
// BeginTransaction() -> Begins a DB transaction.
// CommitTransaction() -> Commits a DB transaction.
// RollbacksTransaction() -> Rollbacks a DB transaction.
// SaveSchemaDependency() -> Insert a schema dependency into the DB.
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
