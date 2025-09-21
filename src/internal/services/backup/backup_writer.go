package backup_services

import "historydb/src/internal/entities"

// BackupWriter is the interface that defines the functionality for writing data into the Backup.
//
// CreateBackupStructure() -> Creates the directory structure used in the backup.
// DeleteBackupStructure() -> Deletes the backup directory.
// BeginSnapshot() -> Begins a transaction for saving all the new snapshot content.
// CommitSnapshot() -> Commits the previous transaction.
// RollbackSnapshot() -> Rollbacks the previous transaction.
// SaveSchemaDependency() -> Saves a schema dependency into the transaction previous created.
// SaveSchemaDependencyDiff() -> Saves a schema dependency reduces version with its updates from the last state.
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
}
