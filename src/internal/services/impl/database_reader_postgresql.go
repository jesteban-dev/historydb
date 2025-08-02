package impl

import (
	"database/sql"
	"fmt"
	"historydb/src/internal/helpers"
	"historydb/src/internal/services"
	"historydb/src/internal/services/impl/entities"
	"historydb/src/internal/services/impl/utils"
	"strings"

	"github.com/lib/pq"
)

// DatabaseReaderPostgreSQL represents the implementation of DatabaseReader for PostgreSQL databases.
type DatabaseReaderPostgreSQL struct {
	db *sql.DB
}

// NewDatabaseReaderPostgreSQL creates a new DatabaseReaderPostgreSQL with the provided database connection.
//
// It returns a pointer to the created DatabaseReaderPostgreSQL.
func NewDatabaseReaderPostgreSQL(db *sql.DB) *DatabaseReaderPostgreSQL {
	return &DatabaseReaderPostgreSQL{db}
}

// ListEntitiesDefinition implements the same function for DatabaseReader interface that returns all metadata from
// the tables in the database.
//
// It returs a slice with all tables metadata and an error if the process fails.
func (dbReader *DatabaseReaderPostgreSQL) ListSchemasDefinition() ([]services.Schema, error) {
	rows, err := dbReader.db.Query(`
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_type = 'BASE TABLE'
		ORDER BY table_catalog, table_schema, table_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemaList := []services.Schema{}
	for rows.Next() {
		var tableSchema string
		var tableName string
		if err := rows.Scan(&tableSchema, &tableName); err != nil {
			return nil, err
		}

		schema, err := dbReader.getTableDefinition(tableSchema, tableName)
		if err != nil {
			return nil, err
		}

		schemaList = append(schemaList, schema)
	}

	return schemaList, nil
}

// GetSchemaDataBatch implements the same function for DatabaseReader interface that a batch of the table rows.
//
// It returs a slice with the content in the data rows of the batch, the updated batch cursor to use in the next call, and an error if the process fail.
func (dbReader *DatabaseReaderPostgreSQL) GetSchemaDataBatch(schema services.Schema, batchSize uint, batchCursor services.BatchCursor) ([]services.SchemaData, services.BatchCursor, error) {
	if schema == nil {
		return nil, nil, ErrNullSchema
	}
	table := schema.(*entities.SQLTable)
	schemaName, tableName := dbReader.parseTableName(table.TableName)

	cursor, ok := batchCursor.(*entities.BatchCursor)
	if !ok || cursor == nil {
		cursor = &entities.BatchCursor{}
	}

	var rows *sql.Rows
	var err error
	pKeys, ok := utils.ExtractPrimaryKey(*table)
	if !ok {
		query := fmt.Sprintf("SELECT * FROM %s.%s ORDER BY ctid LIMIT $2 OFFSET $3", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName))

		rows, err = dbReader.db.Query(query, batchSize, cursor.Offset)
	} else {
		orderClause := fmt.Sprintf("ORDER BY %s", strings.Join(utils.QuoteIdentifiers(pKeys), ", "))

		if cursor.LastPK == nil {
			query := fmt.Sprintf("SELECT * FROM %s.%s %s LIMIT $1", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName), orderClause)
			rows, err = dbReader.db.Query(query, batchSize)
		} else {
			whereClause := utils.BuildPKWhereClause(pKeys, cursor.LastPK)
			query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s %s LIMIT $%d", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName), whereClause, orderClause, len(pKeys)+1)
			args := append(helpers.ToInterfaceSlice(cursor.LastPK), batchSize)
			rows, err = dbReader.db.Query(query, args...)
		}
	}
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	numCols := len(table.Columns)
	var lastPKey []interface{} = nil
	if pKeys != nil {
		lastPKey = make([]interface{}, len(pKeys))
	}

	results := make([]services.SchemaData, 0, batchSize)
	for rows.Next() {
		values := make([]interface{}, numCols)
		valuePtrs := make([]interface{}, numCols)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, err
		}

		row := make(entities.TableRow, numCols)
		for i, col := range table.Columns {
			var val interface{}
			raw := values[i]

			b, ok := raw.([]byte)
			if ok {
				if col.ColumnType == "bytea" {
					val = b
				} else {
					val = string(b)
				}
			} else {
				val = raw
			}

			row[col.ColumnName] = val
		}

		for i, key := range pKeys {
			lastPKey[i] = row[key]
		}

		results = append(results, row)
	}

	cursor.Offset += batchSize
	cursor.LastPK = lastPKey

	return results, cursor, nil
}

// parseTableName is a PostgreSQL specific method that obtains the separated schema and table names from
// the schema name, since in PostgreSQL in necessary to treat them separately.
//
// It receives the full schema.table name.
// It returns the schema and table names.
func (dbReader *DatabaseReaderPostgreSQL) parseTableName(name string) (schema, table string) {
	parts := strings.Split(name, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}

// getTableDefinition is a PostgreSQL specific method that retrieve the definition on a specific table.
// It will retrieve the constraints, foreign keys, indexes and columns from the table.
//
// It receives the schema and name of the table on the database.
// It returns the table definition or metadata and an error if the process fails.
func (dbReader *DatabaseReaderPostgreSQL) getTableDefinition(tableSchema string, tableName string) (services.Schema, error) {
	columns, err := dbReader.extractColumns(tableSchema, tableName)
	if err != nil {
		return nil, err
	}

	constraints, err := dbReader.extractConstraints(tableSchema, tableName)
	if err != nil {
		return nil, err
	}

	foreignKeys, err := dbReader.extractForeignKeys(tableSchema, tableName)
	if err != nil {
		return nil, err
	}

	indexes, err := dbReader.extractIndexes(tableSchema, tableName)
	if err != nil {
		return nil, err
	}

	return &entities.SQLTable{
		TableName:   fmt.Sprintf("%s.%s", tableSchema, tableName),
		Columns:     columns,
		Constraints: constraints,
		ForeignKeys: foreignKeys,
		Indexes:     indexes,
	}, nil
}

// extractColumns is a PostgreSQL specific method that returns the columns used in the specified table.
//
// It receives the schema and name of the table on the database.
// It returns a slice of columns and an error if the process fails.
func (dbReader *DatabaseReaderPostgreSQL) extractColumns(tableSchema string, tableName string) ([]entities.TableColumn, error) {
	rows, err := dbReader.db.Query(`
		SELECT column_name, data_type, CASE is_nullable WHEN 'YES' THEN true ELSE false END AS is_nullable, column_default, ordinal_position, character_maximum_length
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []entities.TableColumn{}
	for rows.Next() {
		var c entities.PSQLColumn
		err := rows.Scan(&c.ColumnName, &c.DataType, &c.IsNullable, &c.ColumnDefault, &c.OrdinalPosition, &c.CharacterMaximumLength)
		if err != nil {
			return nil, err
		}

		if c.CharacterMaximumLength != nil {
			c.DataType = fmt.Sprintf("%s(%d)", c.DataType, *c.CharacterMaximumLength)
		}

		columns = append(columns, entities.TableColumn{
			ColumnName:     c.ColumnName,
			ColumnType:     c.DataType,
			IsNullable:     c.IsNullable,
			DefaultValue:   c.ColumnDefault,
			ColumnPosition: c.OrdinalPosition,
		})
	}

	return columns, nil
}

// extractConstraints is a PostgreSQL specific method that returns the constraints used in the specified table.
//
// It receives the schema and name of the table on the database.
// It returns a slice of constraints and an error if the process fails.
func (dbReader *DatabaseReaderPostgreSQL) extractConstraints(tableSchema string, tableName string) ([]entities.TableConstraint, error) {
	rows, err := dbReader.db.Query(`
		SELECT tc.constraint_name, tc.constraint_type, kcu.column_name, CASE WHEN constraint_type = 'CHECK' THEN substring(pg_get_constraintdef(c.oid) FROM 'CHECK \((.*)\)') ELSE NULL END AS definition
		FROM information_schema.table_constraints tc
			LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			JOIN pg_constraint c ON c.conname = tc.constraint_name
		WHERE tc.table_schema = $1 AND tc.table_name = $2 AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK') AND NOT tc.constraint_name ~ '.*_not_null$'
		ORDER BY tc.constraint_name ASC, kcu.column_name ASC
	`, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraintMap := make(map[string]*entities.TableConstraint)
	for rows.Next() {
		var c entities.PSQLConstraint
		err := rows.Scan(&c.ConstraintName, &c.ConstraintType, &c.ColumnName, &c.Definition)
		if err != nil {
			return nil, err
		}

		tc, exists := constraintMap[c.ConstraintName]
		if !exists {
			var columns []string
			if c.ColumnName != nil {
				columns = append(columns, *c.ColumnName)
			}
			tc = &entities.TableConstraint{
				ConstraintType: entities.ConstraintType(c.ConstraintType),
				ConstraintName: c.ConstraintName,
				Columns:        columns,
				Definition:     c.Definition,
			}

			constraintMap[c.ConstraintName] = tc
		} else if c.ConstraintType != string(entities.Check) && c.ColumnName != nil {
			tc.Columns = append(tc.Columns, *c.ColumnName)
		}
	}

	constraints := make([]entities.TableConstraint, 0, len(constraintMap))
	for _, tc := range constraintMap {
		constraints = append(constraints, *tc)
	}

	return constraints, nil
}

// extractForeignKeys is a PostgreSQL specific method that returns the foreign kets used in the specified table.
//
// It receives the schema and name of the table on the database.
// It returns a slice of foreign keys and an error if the process fails.
func (dbReader *DatabaseReaderPostgreSQL) extractForeignKeys(tableSchema string, tableName string) ([]entities.ForeignKey, error) {
	rows, err := dbReader.db.Query(`
		SELECT tc.constraint_name, kcu.column_name, ccu.table_name AS referenced_table, ccu.column_name AS referenced_column, rc.update_rule, rc.delete_rule
		FROM information_schema.table_constraints tc
			LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name AND tc.constraint_schema = rc.constraint_schema
			JOIN information_schema.constraint_column_usage ccu ON rc.unique_constraint_name = ccu.constraint_name AND rc.unique_constraint_schema = ccu.constraint_schema
		WHERE tc.table_schema = $1 AND tc.table_name = $2 AND tc.constraint_type = 'FOREIGN KEY'
		ORDER BY tc.constraint_name ASC, kcu.column_name ASC
	`, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraintMap := make(map[string]*entities.ForeignKey)
	for rows.Next() {
		var c entities.PSQLForeignKey
		err := rows.Scan(&c.ConstraintName, &c.ColumnName, &c.ReferencedTable, &c.ReferencedColumn, &c.UpdateRule, &c.DeleteRule)
		if err != nil {
			return nil, err
		}

		fk, exists := constraintMap[c.ConstraintName]
		if !exists {
			fk = &entities.ForeignKey{
				ConstraintName:    c.ConstraintName,
				Columns:           []string{c.ColumnName},
				ReferencedTable:   c.ReferencedTable,
				ReferencedColumns: []string{c.ReferencedColumn},
				UpdateAction:      entities.ActionType(c.UpdateRule),
				DeleteAction:      entities.ActionType(c.DeleteRule),
			}

			constraintMap[c.ConstraintName] = fk
		} else {
			fk.Columns = append(fk.Columns, c.ColumnName)
			fk.ReferencedColumns = append(fk.ReferencedColumns, c.ReferencedColumn)
		}
	}

	foreignKeys := make([]entities.ForeignKey, 0, len(constraintMap))
	for _, fk := range constraintMap {
		foreignKeys = append(foreignKeys, *fk)
	}

	return foreignKeys, nil
}

// extractIndexes is a PostgreSQL specific method that returns the indexes used in the specified table.
//
// It receives the schema and name of the table on the database.
// It returns a slice of indexes and an error if the process fails.
func (dbReader *DatabaseReaderPostgreSQL) extractIndexes(tableSchema string, tableName string) ([]entities.Index, error) {
	rows, err := dbReader.db.Query(`
		SELECT pci.relname as index_name, am.amname as index_type, array_agg(a.attname ORDER BY x.ordinality) AS column_names, pi.indisunique as is_unique, pi.indisprimary as is_primary, pg_get_expr(pi.indpred, pi.indrelid) AS partial_condition
		FROM pg_class pc
			JOIN pg_namespace ns ON ns.oid = pc.relnamespace
			JOIN pg_index pi ON pc.oid = pi.indrelid
			JOIN pg_class pci ON pci.oid = pi.indexrelid
			JOIN pg_am am ON pci.relam = am.oid
			JOIN LATERAL UNNEST(pi.indkey) WITH ORDINALITY AS x(attnum, ordinality) ON TRUE
			LEFT JOIN pg_attribute a ON a.attrelid = pc.oid AND a.attnum = x.attnum
		WHERE pc.relkind = 'r' AND ns.nspname = $1 AND pc.relname = $2
		GROUP BY pci.relname, pi.indisunique, pi.indisprimary, am.amname, pi.indpred, pci.oid, pi.indrelid
		ORDER BY pci.relname
	`, tableSchema, tableName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := []entities.Index{}
	for rows.Next() {
		var i entities.PSQLIndex
		err := rows.Scan(&i.IndexName, &i.IndexType, pq.Array(&i.ColumnNames), &i.IsUnique, &i.IsPrimary, &i.PartialCondition)
		if err != nil {
			return nil, err
		}

		options := map[string]interface{}{"isUnique": i.IsUnique, "isPrimary": i.IsPrimary}
		if i.PartialCondition != nil {
			options["partialCondition"] = i.PartialCondition
		}

		indexes = append(indexes, entities.Index{
			IndexName: i.IndexName,
			IndexType: i.IndexType,
			Columns:   i.ColumnNames,
			Options:   options,
		})
	}

	return indexes, nil
}
