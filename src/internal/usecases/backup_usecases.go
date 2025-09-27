package usecases

import "historydb/src/internal/entities"

type BackupUsecases interface {
	GetBackupMetadata() *entities.BackupMetadata
	GetSnapshot(snapshotId string) *entities.BackupSnapshot
	CreateSnapshot(first bool) *entities.BackupSnapshot
	CommitSnapshot(metadata *entities.BackupMetadata, snapshot *entities.BackupSnapshot) bool
	RollbackSnapshot(first bool)

	BackupSchemaDependencies(snapshot *entities.BackupSnapshot) bool
	SnapshotSchemaDependencies(lastSnapshot, snapshot *entities.BackupSnapshot) bool

	BackupSchemas(snapshot *entities.BackupSnapshot) []entities.Schema
	SnapshotSchemas(lastSnapshot, snapshot *entities.BackupSnapshot) []entities.Schema
}
