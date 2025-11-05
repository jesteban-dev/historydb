package usecases

import "historydb/src/internal/entities"

type BackupUsecases interface {
	GetBackupMetadata() *entities.BackupMetadata
	GetSnapshot(snapshotId string) *entities.BackupSnapshot
	CreateSnapshot(first bool) *entities.BackupSnapshot
	CommitSnapshot(metadata *entities.BackupMetadata, snapshot *entities.BackupSnapshot) bool
	RollbackSnapshot(first bool)

	BackupSchemaDependencies(lastSnapshot, snapshot *entities.BackupSnapshot) bool
	BackupSchemas(lastSnapshot, snapshot *entities.BackupSnapshot) []entities.Schema
}
