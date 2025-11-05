package database_impl

import (
	"database/sql"
	"historydb/src/internal/services"
)

type PSQLDatabaseFactory struct {
	db *sql.DB
}

func NewPSQLDatabaseFactory(db *sql.DB) *PSQLDatabaseFactory {
	return &PSQLDatabaseFactory{db}
}

func (factory *PSQLDatabaseFactory) CreateReader() services.DatabaseReader {
	return NewPSQLDatabaseReader(factory.db)
}
