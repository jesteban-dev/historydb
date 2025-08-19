package services

import (
	"errors"
	"historydb/src/internal/services/entities"
)

var (
	ErrBackupExists = errors.New("backup path already exists")
)

// BackupWriter is the main interface component to write the database data into a backup file.
//
// This service is used by the usecases to write all the schema definitions and data into a backup file.
type BackupWriter interface {
	CreateBackupStructure() error
	WriteSchemas(schemas []entities.Schema) error
}
