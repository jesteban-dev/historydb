package usecases

import "historydb/src/internal/entities"

// BackupUsecases is the interface that defines all the functionality to make a backup or a new snapshot in it.
//
// GetBackupMetadata() -> Retrieves the backup metadata if it exists.
// GetSnapshot() -> Retrieves all the snapshot info from a snapshotId.
// CreateSnapshot() -> Creates a new or the first snapshot into the backup.
// CommitSnapshot() -> Commits a snapshot into the backup making it a new stable version.
// RollbackSnapshot() -> Rollbacks the current working snapshot to preserve the last stable version of the backup.
// BackupSchemaDependencies() -> Saves into the backup all the dependencies contained in the DB.
// SnapshotSchemaDependencies() -> Makes a new version of the dependencies contained in the DB by their differences.
// BackupSchemas() -> Saves into the backup all the schemas contained in the DB.
// SnapshotSchemas() -> Makes a new version of the schemas contained in the DB by their defferences.
// BackupSchemaRecords() -> Saves into the backup all the schema data records contained in the DB.
// SnapshotSchemaRecords() -> Makes a new version of the schema data records contained in the DB by their differences.
// BackupRoutines() -> Saves into the backup all the routines contained in the DB.
// SnapshotRoutines() -> Makes a new version of the routines contained in the DB by their differences.
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

	BackupSchemaRecords(snapshot *entities.BackupSnapshot, schema entities.Schema) bool
	SnapshotSchemaRecords(lastSnapshot, snapshot *entities.BackupSnapshot, schema entities.Schema) bool

	BackupRoutines(snapshot *entities.BackupSnapshot) bool
	SnapshotRoutines(lastSnapshot, snapshot *entities.BackupSnapshot) bool
}
