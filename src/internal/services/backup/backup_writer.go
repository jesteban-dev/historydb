package backup_services

import "historydb/src/internal/entities"

// BackupWriter is the interface that defines the functionality for writing data into the Backup.
//
// CreateBackupStructure() -> Creates the directory structure used in the backup.
// DeleteBackupStructure() -> Deletes the backup directory.
// BeginSnapshot() -> Begins a transaction for saving all the new snapshot content.
// CommitSnapshot() -> Commits the previous transaction.
// RollbackSnapshot() -> Rollbacks the previous transaction.
// SaveSchemaDependency() -> Saves a schema dependency into the transaction previously created.
// SaveSchemaDependencyDiff() -> Saves a schema dependency reduced version with its updates from the last state.
// SaveSchema() -> Saves a schema definition into the transaction previously created.
// SaveSchemaDiff() -> Saves a schema definition reduced version with its updates from the last state.
// SaveSchemaRecordChunk() -> Saves a schema record chunk into the transaction previously created.
// SaveSchemaRecordChunkDiff() -> Saves a schema record chunk reduced version with its updates from the last state.
// SaveSchemaRecordBatch() -> Saves a complete schema record batch with all its chunks.
// SaveRoutine() -> Saves a database routine into the transaction previously created.
// SaveSchemaRoutineDiff() -> Saves a database routine reduced version with its updates from the last state.
type BackupWriter interface {
	CreateBackupStructure() error
	DeleteBackupStructure() error

	BeginSnapshot(snapshot *entities.BackupSnapshot) error
	CommitSnapshot(metadata *entities.BackupMetadata) error
	RollbackSnapshot() error

	SaveSchemaDependency(dependency entities.SchemaDependency) error
	SaveSchemaDependencyDiff(diff entities.SchemaDependencyDiff) error

	SaveSchema(schema entities.Schema) error
	SaveSchemaDiff(diff entities.SchemaDiff) error

	SaveSchemaRecordChunk(batchRef string, chunk entities.SchemaRecordChunk) error
	SaveSchemaRecordChunkDiff(prevBatchRef, batchRef string, chunk entities.SchemaRecordChunkDiff) error
	SaveSchemaRecordBatch(batchTempRef, batchRef string) error

	SaveRoutine(routine entities.Routine) error
	SaveRoutineDiff(diff entities.RoutineDiff) error
}
