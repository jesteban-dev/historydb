package services

import (
	"historydb/src/internal/services/backup_impl"
	"historydb/src/internal/services/entities"
)

type BackupFactory interface {
	CreateWriter() BackupWriter
}

// BackupWriter is the main interface component to write the database data into a backup file.
//
// This service is used by the usecases to write all the schema definitions and data into a backup file.
type BackupWriter interface {
	CreateBackupStructure() error
	WriteSchema(schema entities.Schema) error
}

type JSONBackupFactory struct {
	basePath string
}

func NewJSONBackupFactory(basePath string) *JSONBackupFactory {
	return &JSONBackupFactory{basePath}
}

func (factory *JSONBackupFactory) CreateWriter() BackupWriter {
	return backup_impl.NewJSONBackupWriter(factory.basePath)
}
