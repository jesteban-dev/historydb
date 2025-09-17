package database_services

import "historydb/src/internal/entities"

// DatabaseReader is the interface that defines the functionality for querying the DB.
//
// CheckDBIsEmpty() -> Checks the DB is empty. It is used when we desire to save our backup into a DB so we need an empty DB.
// ListSchemaDependencies() -> List the DB dependencies. (Sequences, etc...)
type DatabaseReader interface {
	CheckDBIsEmpty() (bool, error)
	ListSchemaDependencies() ([]entities.SchemaDependency, error)
}
