package services

import "historydb/src/internal/entities"

// DatabaseReader is the interface that defines the components that will read the databases.
type DatabaseReader interface {
	ListSchemaNames() ([]string, error)
	GetSchemaDefinition(schemaName string) (entities.Schema, error)
}
