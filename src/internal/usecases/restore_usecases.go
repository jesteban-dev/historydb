package usecases

import "historydb/src/internal/entities"

// RestoreUsecases is the interface that defines all the functionality to restore a database from a backup.
//
// GetBackupSnapshot() -> Retrieves the backup metadata if it exists.
// BeginDatabaseRestore() -> Begins a database transaction to fully restores the DB.
// CommitDatabaseRestore() -> Commits the database transaction ending successfully the restore process.
// RollbackDatabaseRestore() -> Rollbacks the database transaction ending abruptly the restore process.
// RestoreSchemaDependencies() -> Restore the schema dependencies into the DB from the backup.
// RestoreSchemas() -> Restore the schemas into the DB from the backup.
// RestoreSchemaRules() -> Restore the schema rules and constraints into the DB from the backup.
// RestoreSchemaRecords() -> Restore the schema data records into the DB from the backup.
// RestoreRoutines() -> Restore the routines into the DB from the backup.
type RestoreUsecases interface {
	GetBackupSnapshot(snapshotId *string) *entities.BackupSnapshot
	BeginDatabaseRestore() bool
	CommitDatabaseRestore() bool
	RollbackDatabaseRestore()

	RestoreSchemaDependencies(snapshot *entities.BackupSnapshot) bool
	RestoreSchemas(snapshot *entities.BackupSnapshot) []entities.Schema
	RestoreSchemaRules(snapshot *entities.BackupSnapshot, schemas []entities.Schema) bool
	RestoreSchemaRecords(snapshot *entities.BackupSnapshot, schema entities.Schema) bool
	RestoreRoutines(snapshot *entities.BackupSnapshot) bool
}
