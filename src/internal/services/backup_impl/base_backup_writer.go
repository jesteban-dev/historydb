package backup_impl

import (
	"historydb/src/internal/services/entities"
	"os"
	"path/filepath"
)

type BaseBackupWriter struct {
	basePath string
}

func (writer *BaseBackupWriter) CreateBackupStructure() error {
	_, err := os.Stat(writer.basePath)
	if err == nil {
		return entities.ErrBackupNeedEmptyDir
	}

	if err := os.Mkdir(writer.basePath, 0755); err != nil {
		return err
	}

	err = os.Mkdir(filepath.Join(writer.basePath, "schemas"), 0755)
	return err
}

func (writer *JSONBackupWriter) DeleteBackupStructure() error {
	err := os.RemoveAll(writer.basePath)
	return err
}
