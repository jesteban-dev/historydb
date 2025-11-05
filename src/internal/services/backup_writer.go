package services

import "historydb/src/internal/entities"

// BackupWriter is the interface that defines the components that will write our backup files.
type BackupWriter interface {
	CreateBackupStructure() error
	CreateBackupFile() error
	WriteSchema(schema entities.Schema) error
}
