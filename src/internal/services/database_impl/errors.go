package database_impl

import "errors"

var (
	ErrNullSchema      = errors.New("null schema")
	ErrArgumentParsing = errors.New("impossible to parse argument")
)
