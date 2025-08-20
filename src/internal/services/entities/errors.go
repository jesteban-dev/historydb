package entities

import "errors"

var (
	ErrArgumentParsing = errors.New("impossible to parse argument")
	ErrBackupExists    = errors.New("backup path already exists")
	ErrHashFailed      = errors.New("impossible to create hash")
	ErrNullSchema      = errors.New("null schema")
)
