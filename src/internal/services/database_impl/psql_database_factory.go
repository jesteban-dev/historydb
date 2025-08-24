package database_impl

import (
	"database/sql"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
)

type PSQLDatabaseFactory struct {
	db     *sql.DB
	host   string
	port   int
	dbName string
}

func NewPSQLDatabaseFactory(db *sql.DB, host string, port int, dbName string) *PSQLDatabaseFactory {
	return &PSQLDatabaseFactory{db, host, port, dbName}
}

func (factory *PSQLDatabaseFactory) CreateReader() services.DatabaseReader {
	return NewPSQLDatabaseReader(factory.db)
}

func (factory *PSQLDatabaseFactory) GetDBMetadata() entities.BackupDatabase {
	return entities.BackupDatabase{
		Engine: "postgres",
		Host:   factory.host,
		Port:   factory.port,
		DbName: factory.dbName,
	}
}

func (factory *PSQLDatabaseFactory) CheckBackupDB(db entities.BackupDatabase) bool {
	return db.Engine == "postgres" && db.Host == factory.host && db.Port == factory.port && db.DbName == factory.dbName
}
