package backup_impl

import (
	"historydb/src/internal/services/entities"
	"os"
	"path/filepath"
)

// BaseBackupWriter is a struct that define the repeated logic for all the implementations of the writers.
type BaseBackupWriter struct {
	BasePath string
}

// CreateBackupStructure implements the same function for BackupWriter.
// It will create all the file structure required to save a database backup.
//
// It returns an error if the process fails, or if the structure is already created.
func (writer *BaseBackupWriter) CreateBackupStructure() error {
	info, err := os.Stat(writer.BasePath)
	if err == nil {
		if info.IsDir() {
			return entities.ErrBackupExists
		}

		return err
	}

	if err := os.Mkdir(writer.BasePath, 0755); err != nil {
		return err
	}

	schemasPath := filepath.Join(writer.BasePath, "schemas")
	if err := os.Mkdir(schemasPath, 0775); err != nil {
		return err
	}

	return nil
}
