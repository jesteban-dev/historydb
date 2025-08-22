package backup_impl

import (
	"historydb/src/internal/services"
	"historydb/src/internal/services/entities"
	"os"
	"path/filepath"
)

type BaseBackupWriter struct {
	self     services.BackupWriter
	basePath string
}

func (writer *BaseBackupWriter) CreateBackupStructure() error {
	_, err := os.Stat(writer.basePath)
	if err == nil {
		return entities.ErrBackupExists
	}

	if err := os.Mkdir(writer.basePath, 0755); err != nil {
		return err
	}

	if err := writer.self.CreateBackupFile(); err != nil {
		return err
	}

	schemasPath := filepath.Join(writer.basePath, "schemas")
	err = os.Mkdir(schemasPath, 0755)

	return err
}
