package psql

import (
	"database/sql"
	services "historydb/src/internal/services/database"
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

func (factory *PSQLDatabaseFactory) CreateWriter() services.DatabaseWriter {
	return NewPSQLDatabaseWriter(factory.db)
}

func (factory *PSQLDatabaseFactory) GetDBEngine() string {
	return "postgres"
}

func (factory *PSQLDatabaseFactory) CheckBackupDB(engine string) bool {
	return engine == "postgres"
}
