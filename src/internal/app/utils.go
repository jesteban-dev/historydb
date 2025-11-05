package app

import (
	"database/sql"
	"fmt"
	"historydb/src/internal/services"
	"historydb/src/internal/services/backup_impl"
	"historydb/src/internal/services/database_impl"

	_ "github.com/lib/pq"
)

var supportedBackupActions = map[string]bool{"create": true, "snapshot": true}
var supportedEngines = map[string]string{"postgres": "postgres", "postgresql": "postgres"}

// openDBConnection opens a sql.DB connection from the DSN provided bny the user
func openDBConnection(engine string, dsn string) (*sql.DB, error) {
	db, err := sql.Open(engine, dsn)
	if err != nil {
		fmt.Printf("Impossible to open DB connection.\n")
		return nil, err
	}
	if err := db.Ping(); err != nil {
		fmt.Printf("Could not connect to DB, check sslmode and credentials.\n")
		return nil, err
	}

	return db, nil
}

// createDatabaseFactory creates the implementation of the DatabaseFactory needed for the engine provided by the user
func createDatabaseFactory(engine string, db *sql.DB) services.DatabaseFactory {
	switch engine {
	case "postgres":
		return database_impl.NewPSQLDatabaseFactory(db)
	default:
		return nil
	}
}

// createBackupFactory creates the implementation of the BackupFactory needed
func createBackupFactory(basePath string) services.BackupFactory {
	return backup_impl.NewJSONBackupFactory(basePath)
}
