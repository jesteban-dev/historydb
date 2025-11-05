package services

import "historydb/src/internal/entities"

// DatabaseFactory is the interface that defines a factory for any type of database.
type DatabaseFactory interface {
	CreateReader() DatabaseReader
	GetDBMetadata() entities.BackupDatabase
	CheckBackupDB(db entities.BackupDatabase) bool
}
