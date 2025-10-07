package binary

import backup_services "historydb/src/internal/services/backup"

type BinaryBackupFactory struct {
	backupPath   string
	backupReader *BinaryBackupReader
	backupWriter *BinaryBackupWriter
}

func NewBinaryBackupFactory(backupPath string) *BinaryBackupFactory {
	return &BinaryBackupFactory{backupPath, nil, nil}
}

func (factory *BinaryBackupFactory) CreateReader() backup_services.BackupReader {
	if factory.backupReader == nil {
		factory.backupReader = NewBinaryBackupReader(factory.backupPath)
	}
	return factory.backupReader
}

func (factory *BinaryBackupFactory) CreateWriter() backup_services.BackupWriter {
	if factory.backupWriter == nil {
		factory.backupWriter = NewBinaryBackupWriter(factory.backupPath)
	}
	return factory.backupWriter
}

func (factory *BinaryBackupFactory) GetBackupEncoding() string {
	return "binary"
}
