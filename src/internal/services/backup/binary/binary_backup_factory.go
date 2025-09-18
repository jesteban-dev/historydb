package binary

import backup_services "historydb/src/internal/services/backup"

type BinaryBackupFactory struct {
	backupPath string
}

func NewBinaryBackupFactory(backupPath string) *BinaryBackupFactory {
	return &BinaryBackupFactory{backupPath}
}

func (factory *BinaryBackupFactory) CreateReader() backup_services.BackupReader {
	return NewBinaryBackupReader(factory.backupPath)
}

func (factory *BinaryBackupFactory) CreateWriter() backup_services.BackupWriter {
	return NewBinaryBackupWriter(factory.backupPath)
}

func (factory *BinaryBackupFactory) GetBackupEncoding() string {
	return "binary"
}
