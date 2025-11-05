package entities

import "errors"

var (
	ErrBackupNeedEmptyDir = errors.New("backup path needs to not exist previously")
)
