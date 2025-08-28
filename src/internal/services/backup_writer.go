package services

import "historydb/src/internal/entities"

// BackupWriter is the interface that defines the components that will write our backup files.
//
// CreateBackupStructure creates the backup directory structure.
// DeleteBackupStructure removes the backup directory.
// WriteSchema writes a schema into a schema file.
// WriteSchemaDiff writes a schemaDiff into a schema diff file.
// CommitSnapshot updates the main metadata file.
type BackupWriter interface {
	CreateBackupStructure() error
	DeleteBackupStructure() error
	WriteSchemaDependency(tempPath string, dependency entities.SchemaDependency) error
	WriteSchemaDependencyDiff(tempPath string, dependencyDiff entities.SchemaDependencyDiff) error
	WriteSchema(tempPath string, schema entities.Schema) error
	WriteSchemaDiff(tempPath string, schemaDiff entities.SchemaDiff) error
	CommitSnapshot(tempPath string, metadata entities.BackupMetadata) error
	RollbackSnapshot(tempPath string) error
}
