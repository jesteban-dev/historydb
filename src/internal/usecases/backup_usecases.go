package usecases

import "historydb/src/internal/entities"

type BackupUsecases interface {
	CreateSnapshot(newBackup bool) *entities.BackupSnapshot
	GetBackupMetadata() *entities.BackupMetadata
	BackupSchemaDependencies(snapshot *entities.BackupSnapshot) bool
	SnapshotSchemaDependencies(lastSnapshot, snapshot *entities.BackupSnapshot) bool
	BackupSchemas(snapshot *entities.BackupSnapshot) ([]entities.Schema, bool)
	SnapshotSchemas(lastSnapshot, snapshot *entities.BackupSnapshot) bool
	BackupSchemaData(snapshot *entities.BackupSnapshot, schema entities.Schema) bool
	SnapshotSchemaData(lastSnapshot, snapshot *entities.BackupSnapshot, schema entities.Schema) bool
	CommitSnapshot(backupMetadata *entities.BackupMetadata, snapshot *entities.BackupSnapshot, isNewBackup bool) bool
}
