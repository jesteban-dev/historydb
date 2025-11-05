package services

import (
	"database/sql"
	"historydb/src/internal/services/database_impl"
	"historydb/src/internal/services/entities"
)

type DatabaseFactory interface {
	CreateReader() DatabaseReader
}

// DatabaseReader is the main interface component to read any type of database.
//
// This service is used by the usecases to export the schema definitions and data in an agnostic database system.
type DatabaseReader interface {
	ListSchemas() ([]string, error)
	GetSchemaDefinition(schemaName string) (entities.Schema, error)
	GetDatabaseExtraInfo() (entities.DBExtraInfo, error)
	GetSchemaDataBatch(schema entities.Schema, batchSize uint, batchCursor entities.BatchCursor) ([]entities.SchemaData, entities.BatchCursor, error)
}

type PSQLDatabaseFactory struct {
	db *sql.DB
}

func NewPSQLDatabaseFactory(db *sql.DB) *PSQLDatabaseFactory {
	return &PSQLDatabaseFactory{db}
}

func (factory *PSQLDatabaseFactory) CreateReader() DatabaseReader {
	return database_impl.NewPSQLDatabaseReader(factory.db)
}
