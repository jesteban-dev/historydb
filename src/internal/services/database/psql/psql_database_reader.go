package psql

import (
	"database/sql"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services/entities/psql"

	"github.com/lib/pq"
)

type PSQLDatabaseReader struct {
	db *sql.DB
}

func NewPSQLDatabaseReader(db *sql.DB) *PSQLDatabaseReader {
	return &PSQLDatabaseReader{db}
}

func (reader *PSQLDatabaseReader) CheckDBIsEmpty() (bool, error) {
	var hasObjects bool
	err := reader.db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		)
	`).Scan(&hasObjects)
	return !hasObjects, err
}

func (reader *PSQLDatabaseReader) ListSchemaDependencies() ([]entities.SchemaDependency, error) {
	rows, err := reader.db.Query(`
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
		if err := reader.db.QueryRow(valueQuery).Scan(&lastValue, &isCalled); err != nil {
			return nil, err
		}

		sequences = append(sequences, &psql.PSQLSequence{
			DependencyType: entities.PSQLSequence,
			Version:        psql.CURRENT_VERSION,
			Name:           fmt.Sprintf("%s.%s", sequence_schema, sequence_name),
			Type:           &data_type,
			Start:          &start_value,
			Min:            &minimum_value,
			Max:            &maximum_value,
			Increment:      &increment,
			IsCycle:        &cycle_option,
			LastValue:      &lastValue,
			IsCalled:       &isCalled,
		})
	}

	return sequences, nil
}
