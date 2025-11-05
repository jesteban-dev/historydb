package database_impl

import (
	"database/sql"
	"fmt"
	"historydb/src/internal/entities"
	serv_entities "historydb/src/internal/services/entities"
	"sort"
	"strings"

	"github.com/lib/pq"
)

// PSQLDatabaseReader is the implementation of DatabaseReader for PostgreSQL databases.
type PSQLDatabaseReader struct {
	db *sql.DB
}

func NewPSQLDatabaseReader(db *sql.DB) *PSQLDatabaseReader {
	return &PSQLDatabaseReader{db}
}

func (dbReader *PSQLDatabaseReader) CheckDBIsEmpty() (bool, error) {
	schemas, err := dbReader.ListSchemaNames()
	if err != nil {
		return false, err
	}

	return len(schemas) == 0, nil
}

func (dbReader *PSQLDatabaseReader) ListSchemaNames() ([]string, error) {
	rows, err := dbReader.db.Query(`
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemaNames := []string{}
	for rows.Next() {
		var tableSchema, tableName string
		if err := rows.Scan(&tableSchema, &tableName); err != nil {
			return nil, err
		}

		schemaNames = append(schemaNames, fmt.Sprintf("%s.%s", tableSchema, tableName))
	}

	return schemaNames, nil
}

func (dbReader *PSQLDatabaseReader) ListSchemaDependencies() ([]entities.SchemaDependency, error) {
	rows, err := dbReader.db.Query(`
		SELECT sequence_schema, sequence_name, data_type, start_value::bigint, minimum_value::bigint, maximum_value::bigint, increment::bigint, CASE cycle_option WHEN 'YES' THEN TRUE ELSE FALSE END AS cycle_option
		FROM information_schema.sequences
		ORDER BY sequence_schema, sequence_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sequences := []entities.SchemaDependency{}
	for rows.Next() {
		var sequence_schema, sequence_name, data_type string
		var start_value, minimum_value, maximum_value, increment int
		var cycle_option bool
		if err := rows.Scan(&sequence_schema, &sequence_name, &data_type, &start_value, &minimum_value, &maximum_value, &increment, &cycle_option); err != nil {
			return nil, err
		}

		var lastValue int
		var isCalled bool
		valueQuery := fmt.Sprintf("SELECT last_value, is_called FROM %s.%s", pq.QuoteIdentifier(sequence_schema), pq.QuoteIdentifier(sequence_name))
		if err := dbReader.db.QueryRow(valueQuery).Scan(&lastValue, &isCalled); err != nil {
			return nil, err
		}

		sequences = append(sequences, &serv_entities.PSQLTableSequence{
			Name:      fmt.Sprintf("%s.%s", sequence_schema, sequence_name),
			Type:      &data_type,
			Start:     &start_value,
			Min:       &minimum_value,
			Max:       &maximum_value,
			Increment: &increment,
			IsCycle:   &cycle_option,
			LastValue: &lastValue,
			IsCalled:  &isCalled,
		})
	}

	return sequences, nil
}

func (dbReader *PSQLDatabaseReader) GetSchemaDefinition(schemaName string) (entities.Schema, error) {
	tableSchema, tableName := dbReader.parseTableName(schemaName)

	columns, err := dbReader.extractColumnsFromTable(tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	constraints, err := dbReader.extractConstraintsFromTable(tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	foreignKeys, err := dbReader.extractForeignKeysFromTable(tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	indexes, err := dbReader.extractIndexesFromTable(tableSchema, tableName)
	if err != nil {
		return nil, err
	}

	return &serv_entities.SQLTable{
		SchemaType:  serv_entities.Relational,
		TableName:   schemaName,
		Columns:     columns,
		Constraints: constraints,
		ForeignKeys: foreignKeys,
		Indexes:     indexes,
	}, nil
}

// As PSQL has schemes and our Schema names for this language is composed as <scheme-name>.<table-name>,
// we need this function to obtain the name separately.
func (dbReader *PSQLDatabaseReader) parseTableName(tableName string) (schema, table string) {
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}

// This function is a private PSQL function that extracts the column definitions from a table into the database.
func (dbReader *PSQLDatabaseReader) extractColumnsFromTable(tableSchema, tableName string) ([]serv_entities.SQLTableColumn, error) {
	rows, err := dbReader.db.Query(`
		SELECT column_name, data_type, CASE is_nullable WHEN 'YES' THEN true ELSE false END AS is_nullable, column_default, ordinal_position, character_maximum_length, numeric_precision, numeric_scale
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := []serv_entities.SQLTableColumn{}
	for rows.Next() {
		var columnName, dataType string
		var columnDefault *string
		var isNullable bool
		var ordinalPosition int
		var characterMaximumLength, numericPrecision, numericScale *int
		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &ordinalPosition, &characterMaximumLength, &numericPrecision, &numericScale); err != nil {
			return nil, err
		}

		if characterMaximumLength != nil {
			dataType = fmt.Sprintf("%s(%d)", dataType, *characterMaximumLength)
		}
		if (dataType == "numeric" || dataType == "decimal" || dataType == "real" || dataType == "double precision") && numericPrecision != nil && numericScale != nil {
			dataType = fmt.Sprintf("%s(%d,%d)", dataType, *numericPrecision, *numericScale)
		}

		columns = append(columns, serv_entities.SQLTableColumn{
			Name:         columnName,
			Type:         dataType,
			IsNullable:   isNullable,
			DefaultValue: columnDefault,
			Position:     ordinalPosition,
		})
	}

	return columns, nil
}

// This function is a private PSQL function that extracts the constraint definitions from a table into the database.
func (dbReader *PSQLDatabaseReader) extractConstraintsFromTable(tableSchema, tableName string) ([]serv_entities.SQLTableConstraint, error) {
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

	constraintMap := make(map[string]*serv_entities.SQLTableConstraint)
	for rows.Next() {
		var constraintName, constrainType string
		var columnName, definition *string
		if err := rows.Scan(&constraintName, &constrainType, &columnName, &definition); err != nil {
			return nil, err
		}

		tc, exists := constraintMap[constraintName]
		if !exists {
			var columns []string
			if columnName != nil {
				columns = append(columns, *columnName)
			}

			constraintMap[constraintName] = &serv_entities.SQLTableConstraint{
				Type:       serv_entities.ConstraintType(constrainType),
				Name:       constraintName,
				Columns:    columns,
				Definition: definition,
			}
		} else if serv_entities.ConstraintType(constrainType) != serv_entities.Check && columnName != nil {
			tc.Columns = append(tc.Columns, *columnName)
		}
	}

	keys := make([]string, 0, len(constraintMap))
	for k := range constraintMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	constraints := make([]serv_entities.SQLTableConstraint, 0, len(constraintMap))
	for _, k := range keys {
		constraints = append(constraints, *constraintMap[k])
	}

	return constraints, nil
}

// This function is a private PSQL function that extracts the foreign keys definitions from a table into the database.
func (dbReader *PSQLDatabaseReader) extractForeignKeysFromTable(tableSchema, tableName string) ([]serv_entities.SQLTableForeignKey, error) {
	rows, err := dbReader.db.Query(`
		SELECT tc.constraint_name, kcu.column_name, ccu.table_schema AS referenced_schema, ccu.table_name AS referenced_table, ccu.column_name AS referenced_column, rc.update_rule, rc.delete_rule
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

	constraintMap := make(map[string]*serv_entities.SQLTableForeignKey)
	for rows.Next() {
		var constraintName, columnName, referencedSchema, referencedTable, referencedColumn, updateRule, deleteRule string
		if err := rows.Scan(&constraintName, &columnName, &referencedSchema, &referencedTable, &referencedColumn, &updateRule, &deleteRule); err != nil {
			return nil, err
		}

		fk, exists := constraintMap[constraintName]
		if !exists {
			constraintMap[constraintName] = &serv_entities.SQLTableForeignKey{
				Name:              constraintName,
				Columns:           []string{columnName},
				ReferencedTable:   fmt.Sprintf("%s.%s", referencedSchema, referencedTable),
				ReferencedColumns: []string{referencedColumn},
				UpdateAction:      serv_entities.ActionType(updateRule),
				DeleteAction:      serv_entities.ActionType(deleteRule),
			}
		} else {
			fk.Columns = append(fk.Columns, columnName)
			fk.ReferencedColumns = append(fk.ReferencedColumns, referencedColumn)
		}
	}

	keys := make([]string, 0, len(constraintMap))
	for k := range constraintMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	foreignKeys := make([]serv_entities.SQLTableForeignKey, 0, len(constraintMap))
	for _, k := range keys {
		foreignKeys = append(foreignKeys, *constraintMap[k])
	}

	return foreignKeys, nil
}

// This function is a private PSQL function that extracts the index definitions from a table into the database.
func (dbReader *PSQLDatabaseReader) extractIndexesFromTable(tableSchema, tableName string) ([]serv_entities.SQLTableIndex, error) {
	rows, err := dbReader.db.Query(`
		SELECT pci.relname as index_name, am.amname as index_type, array_agg(a.attname ORDER BY x.ordinality) AS column_names, pi.indisunique as is_unique, pg_get_expr(pi.indpred, pi.indrelid) AS partial_condition
		FROM pg_class pc
			JOIN pg_namespace ns ON ns.oid = pc.relnamespace
			JOIN pg_index pi ON pc.oid = pi.indrelid
			JOIN pg_class pci ON pci.oid = pi.indexrelid
			JOIN pg_am am ON pci.relam = am.oid
			JOIN LATERAL UNNEST(pi.indkey) WITH ORDINALITY AS x(attnum, ordinality) ON TRUE
			LEFT JOIN pg_attribute a ON a.attrelid = pc.oid AND a.attnum = x.attnum
			LEFT JOIN pg_constraint c ON c.conindid = pi.indexrelid
		WHERE pc.relkind = 'r' AND ns.nspname = $1 AND pc.relname = $2 AND c.oid IS NULL
		GROUP BY pci.relname, pi.indisunique, pi.indisprimary, am.amname, pi.indpred, pci.oid, pi.indrelid
		ORDER BY pci.relname
	`, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := []serv_entities.SQLTableIndex{}
	for rows.Next() {
		var indexName, indexType string
		var columnNames []string
		var partialCondition *string
		var isUnique bool
		if err := rows.Scan(&indexName, &indexType, pq.Array(&columnNames), &isUnique, &partialCondition); err != nil {
			return nil, err
		}

		options := map[string]interface{}{"isUnique": isUnique}
		if partialCondition != nil {
			options["partialCondition"] = partialCondition
		}

		indexes = append(indexes, serv_entities.SQLTableIndex{
			Name:    indexName,
			Type:    indexType,
			Columns: columnNames,
			Options: options,
		})
	}

	return indexes, nil
}
