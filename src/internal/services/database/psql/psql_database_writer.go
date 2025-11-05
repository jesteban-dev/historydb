package psql

import (
	"database/sql"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	"historydb/src/internal/services/entities/psql"
	sql_entities "historydb/src/internal/services/entities/sql"
	"strings"
	"time"

	"github.com/lib/pq"
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
		return services.ErrDatabaseTransactionNotFound
	}
	return writer.tx.Commit()
}

func (writer *PSQLDatabaseWriter) RollbackTransaction() error {
	if writer.tx == nil {
		return services.ErrDatabaseTransactionNotFound
	}
	return writer.tx.Rollback()
}

func (writer *PSQLDatabaseWriter) SaveSchemaDependency(dependency entities.SchemaDependency) error {
	if writer.tx == nil {
		return services.ErrDatabaseTransactionNotFound
	}

	sequence := dependency.(*psql.PSQLSequence)
	sequenceSchema, sequenceName := writer.parseDBObjectName(sequence.Name)

	if _, err := writer.tx.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pq.QuoteIdentifier(sequenceSchema))); err != nil {
		return err
	}

	query := fmt.Sprintf(`
		CREATE SEQUENCE %s.%s AS %s
		START %d
		INCREMENT %d
		MINVALUE %d
		MAXVALUE %d
	`, pq.QuoteIdentifier(sequenceSchema), pq.QuoteIdentifier(sequenceName), sequence.Type, sequence.Start, sequence.Increment, sequence.Min, sequence.Max)
	if sequence.IsCycle {
		query += " CYCLE"
	} else {
		query += " NO CYCLE"
	}

	if _, err := writer.tx.Exec(query); err != nil {
		return err
	}

	updateQuery := fmt.Sprintf("ALTER SEQUENCE %s.%s", pq.QuoteIdentifier(sequenceSchema), pq.QuoteIdentifier(sequenceName))
	if sequence.IsCalled {
		updateQuery += fmt.Sprintf(" RESTART WITH %d", sequence.LastValue+sequence.Increment)
	} else {
		updateQuery += fmt.Sprintf(" RESTART WITH %d", sequence.LastValue)
	}

	_, err := writer.tx.Exec(updateQuery)
	return err
}

func (writer *PSQLDatabaseWriter) SaveSchema(schema entities.Schema) error {
	if writer.tx == nil {
		return services.ErrDatabaseTransactionNotFound
	}

	table := schema.(*sql_entities.SQLTable)
	tableSchema, tableName := writer.parseDBObjectName(table.Name)

	if _, err := writer.tx.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pq.QuoteIdentifier(tableSchema))); err != nil {
		return err
	}

	query := fmt.Sprintf("CREATE TABLE %s.%s (", pq.QuoteIdentifier(tableSchema), pq.QuoteIdentifier(tableName))
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
			if c.Type == sql_entities.Check {
				query += fmt.Sprintf(", CONSTRAINT %s %s %s", c.Name, c.Type, *c.Definition)
			} else {
				query += fmt.Sprintf(", CONSTRAINT %s %s (%s)", c.Name, c.Type, strings.Join(c.Columns, ", "))
			}
		}
	}
	query += ");"

	_, err := writer.tx.Exec(query)
	return err
}

func (writer *PSQLDatabaseWriter) SaveSchemaRules(schema entities.Schema) error {
	if writer.tx == nil {
		return services.ErrDatabaseTransactionNotFound
	}

	table := schema.(*sql_entities.SQLTable)
	tableSchema, tableName := writer.parseDBObjectName(table.Name)

	for _, fk := range table.ForeignKeys {
		query := fmt.Sprintf(
			"ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE %s ON DELETE %s;",
			pq.QuoteIdentifier(tableSchema),
			pq.QuoteIdentifier(tableName),
			fk.Name,
			strings.Join(fk.Columns, ", "),
			fk.ReferencedTable,
			strings.Join(fk.ReferencedColumns, ", "),
			fk.UpdateAction,
			fk.DeleteAction,
		)
		if _, err := writer.tx.Exec(query); err != nil {
			return err
		}
	}

	for _, idx := range table.Indexes {
		query := fmt.Sprintf(
			"CREATE INDEX %s ON %s.%s USING %s (%s);",
			idx.Name,
			pq.QuoteIdentifier(tableSchema),
			pq.QuoteIdentifier(tableName),
			idx.Type,
			strings.Join(idx.Columns, ", "),
		)
		if _, err := writer.tx.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

func (writer *PSQLDatabaseWriter) SaveSchemaRecords(schema entities.Schema, chunk entities.SchemaRecordChunk) error {
	if writer.tx == nil {
		return services.ErrDatabaseTransactionNotFound
	}

	table := schema.(*sql_entities.SQLTable)
	tableSchema, tableName := writer.parseDBObjectName(table.Name)

	recordChunk := chunk.(*sql_entities.SQLRecordChunk)

	query := fmt.Sprintf("INSERT INTO %s.%s (", pq.QuoteIdentifier(tableSchema), pq.QuoteIdentifier(tableName))
	for i, col := range table.Columns {
		query += col.Name
		if i < len(table.Columns)-1 {
			query += ", "
		}
	}
	query += ") VALUES "

	for i, record := range recordChunk.Content {
		query += "("
		for j, col := range table.Columns {
			val := record.Content[col.Name]
			switch v := val.(type) {
			case nil:
				query += "NULL"
			case string:
				query += fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
			case time.Time:
				query += fmt.Sprintf("'%s'", v.Format(time.RFC3339))
			default:
				query += fmt.Sprintf("%v", v)
			}

			if j < len(table.Columns)-1 {
				query += ", "
			}
		}
		query += ")"

		if i < len(recordChunk.Content)-1 {
			query += ", "
		}
	}
	query += ";"

	_, err := writer.tx.Exec(query)
	return err
}

func (writer *PSQLDatabaseWriter) SaveRoutine(routine entities.Routine) error {
	if writer.tx == nil {
		return services.ErrDatabaseTransactionNotFound
	}

	var query string
	if routine.GetRoutineType() == entities.PSQLFunction {
		function := routine.(*psql.PSQLFunction)

		query = fmt.Sprintf("CREATE FUNCTION %s(%s) RETURNS %s LANGUAGE %s AS %s %s %s %s", function.Name, function.Parameters, function.ReturnType, function.Language, function.Tag, function.Definition, function.Tag, function.Volatility)
	} else if routine.GetRoutineType() == entities.PSQLProcedure {
		procedure := routine.(*psql.PSQLProcedure)

		query = fmt.Sprintf("CREATE PROCEDURE %s(%s) LANGUAGE %s AS %s %s %s", procedure.Name, procedure.Parameters, procedure.Language, procedure.Tag, procedure.Definition, procedure.Tag)
	} else if routine.GetRoutineType() == entities.PSQLTrigger {
		trigger := routine.(*psql.PSQLTrigger)

		query = fmt.Sprintf("CREATE TRIGGER %s %s", trigger.Name, trigger.Definition)
	} else {
		return services.ErrBackupCorruptedFile
	}

	_, err := writer.tx.Exec(query)
	return err
}

func (writer *PSQLDatabaseWriter) parseDBObjectName(objectName string) (string, string) {
	parts := strings.Split(objectName, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}
