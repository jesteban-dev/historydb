package usecases

import "historydb/src/internal/entities"

type RestoreUsecases interface {
	GetBackupSnapshot(snapshotId *string) *entities.BackupSnapshot
	StartDatabaseRestore() bool
	RestoreSchemaDependencies(snapshot *entities.BackupSnapshot) bool
	RestoreSchemas(snapshot *entities.BackupSnapshot) bool
	EndDatabaseRestore() bool
}
