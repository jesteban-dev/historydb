package psql

import (
	"database/sql"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services/entities/psql"
	"strings"
)

type PSQLDatabaseWriter struct {
	db *sql.DB
	tx *sql.Tx
}

func NewPSQLDatabaseWriter(db *sql.DB) *PSQLDatabaseWriter {
	return &PSQLDatabaseWriter{db: db}
}

func (writer *PSQLDatabaseWriter) BeginTransaction() error {
	var err error
	writer.tx, err = writer.db.Begin()
	return err
}

func (writer *PSQLDatabaseWriter) CommitTransaction() error {
	if writer.tx == nil {
		return fmt.Errorf("no db transaction in progress")
	}
	return writer.tx.Commit()
}

func (writer *PSQLDatabaseWriter) RollbackTransaction() error {
	if writer.tx == nil {
		return fmt.Errorf("no db transaction in progress")
	}
	return writer.tx.Rollback()
}

func (writer *PSQLDatabaseWriter) SaveSchemaDependency(dependency entities.SchemaDependency) error {
	sequence := dependency.(*psql.PSQLSequence)
	sequenceSchema, sequenceName := writer.parseDBObjectName(sequence.Name)

	if _, err := writer.tx.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", sequenceSchema)); err != nil {
		return err
	}

	query := fmt.Sprintf(`
		CREATE SEQUENCE %s.%s AS %s
		START %d
		INCREMENT %d
		MINVALUE %d
		MAXVALUE %d
	`, sequenceSchema, sequenceName, *sequence.Type, *sequence.Start, *sequence.Increment, *sequence.Min, *sequence.Max)
	if *sequence.IsCycle {
		query += " CYCLE"
	} else {
		query += " NO CYCLE"
	}

	if _, err := writer.tx.Exec(query); err != nil {
		return err
	}

	updateQuery := fmt.Sprintf("ALTER SEQUENCE %s.%s", sequenceSchema, sequenceName)
	if *sequence.IsCalled {
		updateQuery += fmt.Sprintf(" RESTART WITH %d", *sequence.LastValue+*sequence.Increment)
	} else {
		updateQuery += fmt.Sprintf(" RESTART WITH %d", *sequence.LastValue)
	}

	_, err := writer.tx.Exec(updateQuery)
	return err
}

func (writer *PSQLDatabaseWriter) parseDBObjectName(objectName string) (string, string) {
	parts := strings.Split(objectName, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}
