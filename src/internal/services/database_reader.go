package services

import "historydb/src/internal/entities"

// DatabaseReader is the interface that defines the components that will read the databases.
//
// ListSchemaNames obtaines all the schema names from the database.
// GetSchemaDefinition obtaines the schema from the database given a schema name.
type DatabaseReader interface {
	ListSchemaNames() ([]string, error)
	GetSchemaDefinition(schemaName string) (entities.Schema, error)
}
