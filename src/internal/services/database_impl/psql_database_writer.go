package database_impl

import (
	"database/sql"
	"fmt"
	"historydb/src/internal/entities"
	serv_entities "historydb/src/internal/services/entities"
	"strings"
)

// PSQLDatabaseWriter is the implementation of DatabaseQriter for PostgreSQL databases.
type PSQLDatabaseWriter struct {
	db *sql.DB
	tx *sql.Tx
}

func NewPSQLDatabaseWriter(db *sql.DB) *PSQLDatabaseWriter {
	return &PSQLDatabaseWriter{db: db}
}

func (dbWriter *PSQLDatabaseWriter) BeginTransaction() error {
	var err error
	dbWriter.tx, err = dbWriter.db.Begin()
	return err
}

func (dbWriter *PSQLDatabaseWriter) WriteSchemaDependency(dependency entities.SchemaDependency) error {
	sequence := dependency.(*serv_entities.PSQLTableSequence)
	sequenceSchema, sequenceName := dbWriter.parseDBObjectName(sequence.Name)

	if _, err := dbWriter.tx.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", sequenceSchema)); err != nil {
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

	if _, err := dbWriter.tx.Exec(query); err != nil {
		return err
	}

	updateQuery := fmt.Sprintf("ALTER SEQUENCE %s.%s", sequenceSchema, sequenceName)
	if *sequence.IsCalled {
		updateQuery += fmt.Sprintf(" RESTART WITH %d", *sequence.LastValue+*sequence.Increment)
	} else {
		updateQuery += fmt.Sprintf(" RESTART WITH %d", *sequence.LastValue)
	}

	_, err := dbWriter.tx.Exec(updateQuery)
	return err
}

func (dbWriter *PSQLDatabaseWriter) WriteSchema(schema entities.Schema) error {
	table := schema.(*serv_entities.SQLTable)
	tableSchema, tableName := dbWriter.parseDBObjectName(table.TableName)

	if _, err := dbWriter.tx.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", tableSchema)); err != nil {
		return err
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (", tableSchema, tableName)
	for i, col := range table.Columns {
		query += fmt.Sprintf("%s %s", col.Name, col.Type)
		if !col.IsNullable {
			query += " NOT NULL"
		}
		if col.DefaultValue != nil {
			query += fmt.Sprintf(" DEFAULT %s", *col.DefaultValue)
		}
		if i < len(table.Columns)-1 {
			query += ", "
		}
	}
	if len(table.Constraints) > 0 {
		for _, c := range table.Constraints {
			if c.Type == serv_entities.Check {
				query += fmt.Sprintf(", CONSTRAINT %s %s %s", c.Name, c.Type, *c.Definition)
			} else {
				query += fmt.Sprintf(", CONSTRAINT %s %s (%s)", c.Name, c.Type, strings.Join(c.Columns, ", "))
			}
		}
	}
	query += ");"
	fmt.Println(query)

	_, err := dbWriter.tx.Exec(query)
	return err
}

func (dbWriter *PSQLDatabaseWriter) WriteSchemaRules(schema entities.Schema) error {
	table := schema.(*serv_entities.SQLTable)

	for _, fk := range table.ForeignKeys {
		query := fmt.Sprintf(
			"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE %s ON DELETE %s;",
			table.TableName,
			fk.Name,
			strings.Join(fk.Columns, ", "),
			fk.ReferencedTable,
			strings.Join(fk.ReferencedColumns, ", "),
			fk.UpdateAction,
			fk.DeleteAction,
		)
		if _, err := dbWriter.tx.Exec(query); err != nil {
			return err
		}
	}

	for _, idx := range table.Indexes {
		query := fmt.Sprintf(
			"CREATE INDEX %s ON %s USING %s (%s);",
			idx.Name,
			table.TableName,
			idx.Type,
			strings.Join(idx.Columns, ", "),
		)
		if _, err := dbWriter.tx.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

func (dbWriter *PSQLDatabaseWriter) CommitTransaction() error {
	if dbWriter.tx == nil {
		return fmt.Errorf("there is no transaction in progress")
	}
	return dbWriter.tx.Commit()
}

func (dbWriter *PSQLDatabaseWriter) RollbackTransaction() error {
	if dbWriter.tx == nil {
		return fmt.Errorf("there is no transaction in progress")
	}
	return dbWriter.tx.Rollback()
}

// As PSQL has schemes and our Schema names for this language is composed as <scheme-name>.<table-name>,
// we need this function to obtain the name separately.
func (dbWriter *PSQLDatabaseWriter) parseDBObjectName(tableName string) (schema, table string) {
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}
