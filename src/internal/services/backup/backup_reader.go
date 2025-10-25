package backup_services

import "historydb/src/internal/entities"

// BackupReader is the interface that defines the functionality for retrieving data from the Backup.
//
// CheckBackupExists() -> Checks if a backup in the specified path already exists.
// GetBackupMetadata() -> Retrieves the metadata from the main backup file.
// GetBackupSnapshot() -> Retrieves the snapshot from the main backup file.
// GetSchemaDependency() -> Retrieves the current state of an schema dependency from its backup files.
// GetSchema() -> Retrieves the current state of an schema from its backup files.
// GetSchemaRecordChunkRefsInBatch() -> Retrieves a list of the chunk references contained by a batch from it backup file.
// GetSchemaRecordChunk() -> Retrieves a data chunk from its backup files.
// GetRoutine() -> Retrieves the current state of a routine from its backup files.
type BackupReader interface {
	CheckBackupExists() bool
	GetBackupMetadata() (entities.BackupMetadata, error)
	GetBackupSnapshot(snapshotId string) (entities.BackupSnapshot, error)
	GetSchemaDependency(dependencyRef string) (entities.SchemaDependency, bool, error)
	GetSchema(schemaRef string) (entities.Schema, bool, error)
	GetSchemaRecordChunkRefsInBatch(batchRef string) ([]string, error)
	GetSchemaRecordChunk(batchRef, chunkRef string) (entities.SchemaRecordChunk, bool, error)
	GetRoutine(routineRef string) (entities.Routine, bool, error)
}
