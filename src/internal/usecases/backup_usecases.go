package usecases

import (
	"historydb/src/internal/services"
)

type BackupUsecases struct {
	dbFactory     services.DatabaseFactory
	backupFactory services.BackupFactory
}

func NewBackupUsecases(dbFactory services.DatabaseFactory, backupFactory services.BackupFactory) *BackupUsecases {
	return &BackupUsecases{dbFactory, backupFactory}
}

func (uc *BackupUsecases) CreateBackup() {

}
