package services

import "errors"

var (
	ErrBackupChunkNotFound               = errors.New("backup record chunk not found")
	ErrBackupCorruptedFile               = errors.New("backup file is corrupted")
	ErrBackupDirNotExists                = errors.New("backup directory not exists")
	ErrBackupTransactionInProgress       = errors.New("backup transaction is already in progress")
	ErrBackupTransactionNotFound         = errors.New("no backup transaction in progress")
	ErrDatabaseTransactionAlreadyStarted = errors.New("database transaction already started")
	ErrDatabaseTransactionNotFound       = errors.New("no db transaction in progress")
	ErrDependencyNotSupported            = errors.New("unsupported schema dependency type")
	ErrRecordNotSupported                = errors.New("unsupported schema record type")
	ErrRoutineNotSupported               = errors.New("unsupported routine type")
	ErrSchemaNotSupported                = errors.New("unsupported schema type")
)
