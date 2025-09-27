package usecases

import "historydb/src/internal/entities"

type RestoreUsecases interface {
	GetBackupSnapshot(snapshotId *string) *entities.BackupSnapshot
	BeginDatabaseRestore() bool
	CommitDatabaseRestore() bool
	RollbackDatabaseRestore()

	RestoreSchemaDependencies(snapshot *entities.BackupSnapshot) bool
	RestoreSchemas(snapshot *entities.BackupSnapshot) bool
}
