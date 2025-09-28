package backup_services

import "historydb/src/internal/entities"

// BackupReader is the interface that defines the functionality for retrieving data from the Backup.
//
// GetBackupMetadata() -> Retrieves the metadata from the main backup file.
// GetBackupSnapshot() -> Retrieves the snapshot from the main backup file.
// GetSchemaDependency() -> Retrieves the current state of an schema dependency from its backup file.
type BackupReader interface {
	GetBackupMetadata() (entities.BackupMetadata, error)
	GetBackupSnapshot(snapshotId string) (entities.BackupSnapshot, error)
	GetSchemaDependency(dependencyRef string) (entities.SchemaDependency, bool, error)
	GetSchema(filename string) (entities.Schema, bool, error)
}
