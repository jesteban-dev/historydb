package utils

import (
	"fmt"
	"historydb/src/internal/services/database_impl/entities"
	"strings"

	"github.com/lib/pq"
)

// QuoteIdentifiers is a function used to quote in PostgreSQL all strings needed.
// Mainly used to quote all primary key columns if it is a composed key.
//
// Using PostgreSQL specific library. Should be moved to a specific PostgreSQL file?????
func QuoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, ident := range identifiers {
		quoted[i] = pq.QuoteIdentifier(ident)
	}
	return quoted
}

// BuildPKWhereClause is a function that creates a where clause when batching data queries using the primary key of the table.
//
// Using PostgreSQL specific library. Should be moved to a specific PostgreSQL file?????
func BuildPKWhereClause(pKeys []string, lastPK interface{}) string {
	if len(pKeys) == 1 {
		return fmt.Sprintf("%s > $1", pq.QuoteIdentifier(pKeys[0]))
	}

	placeholders := make([]string, len(pKeys))
	for i := range pKeys {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf("(%s) > (%s)", strings.Join(QuoteIdentifiers(pKeys), ", "), strings.Join(placeholders, ", "))
}

// ExtractPrimaryKey obtains the primary key column names from the primary key of the table.
//
// It receives the SQL table.
// It returns a slice of column names used in the primary key, and a bool indicating if the columns retrieved
// or not are compatible to use in a batch query by primary key.
func ExtractPrimaryKey(table entities.SQLTable) ([]string, bool) {
	var pKeys []string
	for _, constraint := range table.Constraints {
		if constraint.ConstraintType == entities.PrimaryKey {
			pKeys = constraint.Columns
		}
	}

	if len(pKeys) == 0 {
		return nil, false
	}

	columnsMap := make(map[string]string)
	for _, column := range table.Columns {
		columnsMap[column.ColumnName] = column.ColumnType
	}

	for _, column := range pKeys {
		if dataType, exists := columnsMap[column]; exists {
			if _, exists := entities.ComparablePK[dataType]; !exists {
				return nil, false
			}
		}
	}

	return pKeys, true
}
