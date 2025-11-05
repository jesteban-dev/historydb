package entities

import "errors"

var (
	ErrBackupExists = errors.New("backup path already exists")
)
