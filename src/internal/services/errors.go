package services

import "errors"

var (
	ErrBackupDirExists = errors.New("backup directory already exists")
)
