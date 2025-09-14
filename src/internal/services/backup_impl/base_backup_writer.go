package backup_impl

import (
	"historydb/src/internal/services"
	"os"
	"path/filepath"
)

type BaseBackupWriter struct {
	basePath string
}

func (writer *BaseBackupWriter) CreateBackupStructure() error {
	_, err := os.Stat(writer.basePath)
	if err == nil {
		return services.ErrBackupDirExists
	}

	if err := os.Mkdir(writer.basePath, 0755); err != nil {
		return err
	}

	if err := os.Mkdir(filepath.Join(writer.basePath, "schemas"), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(writer.basePath, "schemas", "dependencies"), 0755); err != nil {
		return err
	}
	return os.Mkdir(filepath.Join(writer.basePath, "data"), 0755)
}

func (writer *JSONBackupWriter) DeleteBackupStructure() error {
	return os.RemoveAll(writer.basePath)
}
