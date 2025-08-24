package backup_impl

import (
	"historydb/src/internal/services"
)

type JSONBackupFactory struct {
	basePath string
}

func NewJSONBackupFactory(basePath string) *JSONBackupFactory {
	return &JSONBackupFactory{basePath}
}

func (factory *JSONBackupFactory) CreateReader() services.BackupReader {
	return NewJSONBackupReader(factory.basePath)
}

func (factory *JSONBackupFactory) CreateWriter() services.BackupWriter {
	return NewJSONBackupWriter(factory.basePath)
}
