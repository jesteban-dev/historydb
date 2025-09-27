package services

import "errors"

var (
	ErrBackupCorruptedFile         = errors.New("backup file is corrupted")
	ErrBackupDirExists             = errors.New("backup directory already exists")
	ErrBackupDirNotExists          = errors.New("backup directory not exists")
	ErrBackupTransactionInProgress = errors.New("backup transaction is already in progress")
	ErrBackupTransactionNotFound   = errors.New("no backup transaction in progress")
	ErrDatabaseTransactionNotFound = errors.New("no db transaction in progress")
	ErrDependencyNotSupported      = errors.New("unsupported schema dependency type")
	ErrSchemaNotSupported          = errors.New("unsupported schema type")
)
