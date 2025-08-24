package services

import "historydb/src/internal/entities"

// BackupReader is the interface that defines the components that will read from our backup files.
//
// ReadBackupMetadata retrieves the backup metadata from the main metadata file.
// ReadSchema retrieve the full schema definition info from a schema file. If it is a diff file, it will build the full schema.
type BackupReader interface {
	ReadBackupMetadata() (entities.BackupMetadata, error)
	ReadSchema(hash string) (entities.Schema, error)
}
