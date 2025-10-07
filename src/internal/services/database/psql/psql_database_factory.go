package psql

import (
	"database/sql"
	database_services "historydb/src/internal/services/database"
)

type PSQLDatabaseFactory struct {
	db       *sql.DB
	dbReader *PSQLDatabaseReader
	dbWriter *PSQLDatabaseWriter
}

func NewPSQLDatabaseFactory(db *sql.DB) *PSQLDatabaseFactory {
	return &PSQLDatabaseFactory{db, nil, nil}
}

func (factory *PSQLDatabaseFactory) CreateReader() database_services.DatabaseReader {
	if factory.dbReader == nil {
		factory.dbReader = NewPSQLDatabaseReader(factory.db)
	}
	return factory.dbReader
}

func (factory *PSQLDatabaseFactory) CreateWriter() database_services.DatabaseWriter {
	if factory.dbWriter == nil {
		factory.dbWriter = NewPSQLDatabaseWriter(factory.db)
	}
	return factory.dbWriter
}

func (factory *PSQLDatabaseFactory) GetDBEngine() string {
	return "postgres"
}

func (factory *PSQLDatabaseFactory) CheckBackupDB(engine string) bool {
	return engine == "postgres"
}
