package test

import (
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services/backup/binary"
	"historydb/src/internal/services/entities/psql"
	"historydb/src/internal/services/entities/sql"
	"historydb/src/internal/utils/pointers"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type SchemaDependecyInfo struct {
	dependency entities.SchemaDependency
	toDiff     []entities.SchemaDependency
}

type SchemaInfo struct {
	schema entities.Schema
	toDiff []entities.Schema
}

type RecordBatchInfo struct {
	batchName   string
	chunks      []entities.SchemaRecordChunk
	toDiffNames []string
	toDiff      [][]entities.SchemaRecordChunk
}

type RoutineInfo struct {
	routine entities.Routine
	toDiff  []entities.Routine
}

var expectedDependencies map[string]entities.SchemaDependency = map[string]entities.SchemaDependency{
	"Sequence1": &psql.PSQLSequence{Version: 1, Name: "Sequence1", Type: "integer", Start: 1, Min: 1, Max: 1000, Increment: 1, IsCycle: false, LastValue: 20, IsCalled: false},
	"Sequence2": &psql.PSQLSequence{Version: 1, Name: "Sequence2", Type: "integer", Start: 20, Min: 2, Max: 100, Increment: 2, IsCycle: false, LastValue: 40, IsCalled: true},
	"Sequence3": &psql.PSQLSequence{Version: 1, Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 3, IsCycle: false, LastValue: 20, IsCalled: true},
}

var expectedSchemas map[string]entities.Schema = map[string]entities.Schema{
	"Table1": &sql.SQLTable{
		Version: 1,
		Name:    "Table1",
		Columns: []sql.SQLTableColumn{
			{Name: "Column1", Type: "integer", IsNullable: false, DefaultValue: pointers.Ptr("1"), Position: 1},
			{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 2},
		},
		Constraints: []sql.SQLTableConstraint{
			{Type: sql.PrimaryKey, Name: "Column1_pk", Columns: []string{"Column1"}},
		},
	},
	"Table2": &sql.SQLTable{
		Version: 1,
		Name:    "Table2",
		Columns: []sql.SQLTableColumn{
			{Name: "Column1", Type: "timestamptz", IsNullable: true, Position: 1},
			{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 2},
			{Name: "Column3", Type: "bigint", IsNullable: true, Position: 3},
		},
		Constraints: []sql.SQLTableConstraint{
			{Type: sql.PrimaryKey, Name: "Column1_pk", Columns: []string{"Column1"}},
		},
		ForeignKeys: []sql.SQLTableForeignKey{
			{Name: "Table2_FK", Columns: []string{"Column3"}, ReferencedTable: "Table1", ReferencedColumns: []string{"Column1"}, UpdateAction: sql.NoAction, DeleteAction: sql.NoAction},
		},
		Indexes: []sql.SQLTableIndex{
			{Name: "Table3_IDX", Type: "btree", Columns: []string{"Column1"}, Options: map[string]interface{}{"test1": int64(1)}},
		},
	},
	"Table3": &sql.SQLTable{
		Version: 1,
		Name:    "Table2",
		Columns: []sql.SQLTableColumn{
			{Name: "Column1", Type: "timestamptz", IsNullable: true, Position: 1},
			{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 2},
			{Name: "Column3", Type: "bigint", IsNullable: true, Position: 3},
			{Name: "Column4", Type: "boolean", IsNullable: true, Position: 4},
		},
		Constraints: []sql.SQLTableConstraint{
			{Type: sql.PrimaryKey, Name: "Column1_pk", Columns: []string{"Column1"}},
		},
		ForeignKeys: []sql.SQLTableForeignKey{
			{Name: "Table2_FK", Columns: []string{"Column3"}, ReferencedTable: "Table1", ReferencedColumns: []string{"Column1"}, UpdateAction: sql.NoAction, DeleteAction: sql.NoAction},
		},
		Indexes: []sql.SQLTableIndex{
			{Name: "Table3_IDX", Type: "btree", Columns: []string{"Column1"}, Options: map[string]interface{}{"test1": int64(1)}},
		},
	},
}

var expectedRecordBatch map[string][]RecordBatchInfo = map[string][]RecordBatchInfo{
	"Table1": {
		{batchName: "Batch1-1", chunks: []entities.SchemaRecordChunk{
			&sql.SQLRecordChunk{Content: []sql.SQLRecord{
				{Content: map[string]interface{}{"Column1": int64(1), "Column2": "test1"}},
				{Content: map[string]interface{}{"Column1": int64(2), "Column2": "test2"}},
			}},
			&sql.SQLRecordChunk{Content: []sql.SQLRecord{
				{Content: map[string]interface{}{"Column1": int64(3), "Column2": "test3"}},
				{Content: map[string]interface{}{"Column1": int64(4), "Column2": "test4"}},
			}},
		}},
		{batchName: "Batch1-2", chunks: []entities.SchemaRecordChunk{
			&sql.SQLRecordChunk{Content: []sql.SQLRecord{
				{Content: map[string]interface{}{"Column1": int64(5), "Column2": "test5"}},
				{Content: map[string]interface{}{"Column1": int64(6), "Column2": "test6"}},
			}},
		}},
	},
	"Table2": {
		{batchName: "Batch2-1", chunks: []entities.SchemaRecordChunk{
			&sql.SQLRecordChunk{Content: []sql.SQLRecord{
				{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test1", "Column3": int64(1)}},
				{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test3", "Column3": int64(3)}},
				{Content: map[string]interface{}{"Column1": time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test4", "Column3": int64(4)}},
			}},
		}},
	},
	"Table3": {
		{batchName: "Batch3-1", chunks: []entities.SchemaRecordChunk{
			&sql.SQLRecordChunk{Content: []sql.SQLRecord{
				{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test2", "Column3": int64(2), "Column4": true}},
				{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test10", "Column3": int64(10), "Column4": false}},
			}},
			&sql.SQLRecordChunk{Content: []sql.SQLRecord{
				{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test3", "Column3": int64(3), "Column4": true}},
				{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test4", "Column3": int64(4), "Column4": true}},
				{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test11", "Column3": int64(11), "Column4": false}},
			}},
		}},
	},
}

var expectedRoutines map[string]entities.Routine = map[string]entities.Routine{
	"Function1":  &psql.PSQLFunction{Version: 1, Name: "Function1", Language: "pgplpsql", Volatility: pointers.Ptr("STATIC"), Parameters: pointers.Ptr("p1 string, p2 string"), ReturnType: "integer", Tag: "$$", Definition: "RETURN LEN(p1) + LEN(p2)"},
	"Function2":  &psql.PSQLFunction{Version: 1, Name: "Function2", Language: "pgplpsql", Dependencies: []string{"Function1"}, Parameters: nil, ReturnType: "integer", Tag: "$$function$$", Definition: "RETURN Function1(test)"},
	"Procedure1": &psql.PSQLProcedure{Version: 1, Name: "Procedure1", Language: "sql", Tag: "$$", Definition: "LOOP 1 TO 1000"},
	"Procedure2": &psql.PSQLProcedure{Version: 1, Name: "Procedure2", Language: "sql", Dependencies: []string{"Procedure1"}, Parameters: pointers.Ptr("string, string"), Tag: "$$", Definition: "DO $1 - $2 AND Procedure1()"},
	"Trigger1":   &psql.PSQLTrigger{Version: 1, Name: "Trigger1", Definition: "DO LOOP 1 TO 50 AND SUM(50, 20)"},
	"Trigger2":   &psql.PSQLTrigger{Version: 1, Name: "Trigger2", Definition: "THIS IS THE SECOND TRIGGER DEFINITION"},
}

func TestBinaryBackup(t *testing.T) {
	backupFactory := binary.NewBinaryBackupFactory("test")
	assert.NotNil(t, backupFactory)

	backupReader := backupFactory.CreateReader()
	backupWriter := backupFactory.CreateWriter()
	assert.NotNil(t, backupReader)
	assert.NotNil(t, backupWriter)
	assert.Equal(t, "binary", backupFactory.GetBackupEncoding())

	err := backupWriter.CreateBackupStructure()
	assert.Nil(t, err)

	// Writing snapshot
	metadata := entities.BackupMetadata{
		DatabaseEngine: "test",
	}

	rollbackSnapshot := entities.BackupSnapshot{
		Timestamp:  time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		SnapshotId: "rollback-snapshot",
	}
	snapshot := entities.BackupSnapshot{
		Timestamp:          time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		SnapshotId:         "test-snapshot",
		SchemaDependencies: make(map[string]string),
		Schemas:            make(map[string]string),
		Data:               make(map[string]entities.BackupSnapshotSchemaData),
		Routines:           make(map[string]string),
	}

	dependenciesMap := map[string]SchemaDependecyInfo{
		"Sequence1": {
			dependency: &psql.PSQLSequence{Version: 1, Name: "Sequence1", Type: "integer", Start: 1, Min: 1, Max: 1000, Increment: 1, IsCycle: false, LastValue: 20, IsCalled: false},
		},
		"Sequence2": {
			dependency: &psql.PSQLSequence{Version: 1, Name: "Sequence2", Type: "integer", Start: 20, Min: 2, Max: 100, Increment: 2, IsCycle: false, LastValue: 30, IsCalled: true},
			toDiff: []entities.SchemaDependency{
				&psql.PSQLSequence{Version: 1, Name: "Sequence2", Type: "integer", Start: 20, Min: 2, Max: 100, Increment: 2, IsCycle: false, LastValue: 40, IsCalled: true},
			},
		},
		"Sequence3": {
			dependency: &psql.PSQLSequence{Version: 1, Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 2, IsCycle: false, LastValue: 5, IsCalled: false},
			toDiff: []entities.SchemaDependency{
				&psql.PSQLSequence{Version: 1, Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 3, IsCycle: false, LastValue: 10, IsCalled: true},
				&psql.PSQLSequence{Version: 1, Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 3, IsCycle: false, LastValue: 20, IsCalled: true},
			},
		},
	}
	schemasMap := map[string]SchemaInfo{
		"Table1": {
			schema: &sql.SQLTable{
				Version: 1,
				Name:    "Table1",
				Columns: []sql.SQLTableColumn{
					{Name: "Column1", Type: "integer", IsNullable: false, DefaultValue: pointers.Ptr("1"), Position: 1},
					{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 2},
				},
				Constraints: []sql.SQLTableConstraint{
					{Type: sql.PrimaryKey, Name: "Column1_pk", Columns: []string{"Column1"}},
				},
			},
		},
		"Table2": {
			schema: &sql.SQLTable{
				Version: 1,
				Name:    "Table2",
				Columns: []sql.SQLTableColumn{
					{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 1},
					{Name: "Column3", Type: "bigint", IsNullable: true, Position: 2},
				},
				ForeignKeys: []sql.SQLTableForeignKey{
					{Name: "Table2_FK", Columns: []string{"Column3"}, ReferencedTable: "Table1", ReferencedColumns: []string{"Column1"}, UpdateAction: sql.NoAction, DeleteAction: sql.NoAction},
				},
			},
			toDiff: []entities.Schema{
				&sql.SQLTable{
					Version: 1,
					Name:    "Table2",
					Columns: []sql.SQLTableColumn{
						{Name: "Column1", Type: "timestamptz", IsNullable: true, Position: 1},
						{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 2},
						{Name: "Column3", Type: "bigint", IsNullable: true, Position: 3},
					},
					Constraints: []sql.SQLTableConstraint{
						{Type: sql.PrimaryKey, Name: "Column1_pk", Columns: []string{"Column1"}},
					},
					ForeignKeys: []sql.SQLTableForeignKey{
						{Name: "Table2_FK", Columns: []string{"Column3"}, ReferencedTable: "Table1", ReferencedColumns: []string{"Column1"}, UpdateAction: sql.NoAction, DeleteAction: sql.NoAction},
					},
					Indexes: []sql.SQLTableIndex{
						{Name: "Table3_IDX", Type: "btree", Columns: []string{"Column1"}, Options: map[string]interface{}{"test1": 1}},
					},
				},
			},
		},
		"Table3": {
			schema: &sql.SQLTable{
				Version: 1,
				Name:    "Table2",
				Columns: []sql.SQLTableColumn{
					{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 1},
					{Name: "Column3", Type: "bigint", IsNullable: true, Position: 2},
				},
				ForeignKeys: []sql.SQLTableForeignKey{
					{Name: "Table2_FK", Columns: []string{"Column3"}, ReferencedTable: "Table1", ReferencedColumns: []string{"Column1"}, UpdateAction: sql.NoAction, DeleteAction: sql.NoAction},
				},
			},
			toDiff: []entities.Schema{
				&sql.SQLTable{
					Version: 1,
					Name:    "Table2",
					Columns: []sql.SQLTableColumn{
						{Name: "Column1", Type: "timestamptz", IsNullable: true, Position: 1},
						{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 2},
						{Name: "Column3", Type: "bigint", IsNullable: true, Position: 3},
					},
					Constraints: []sql.SQLTableConstraint{
						{Type: sql.PrimaryKey, Name: "Column1_pk", Columns: []string{"Column1"}},
					},
					ForeignKeys: []sql.SQLTableForeignKey{
						{Name: "Table2_FK", Columns: []string{"Column3"}, ReferencedTable: "Table1", ReferencedColumns: []string{"Column1"}, UpdateAction: sql.NoAction, DeleteAction: sql.NoAction},
					},
					Indexes: []sql.SQLTableIndex{
						{Name: "Table3_IDX", Type: "btree", Columns: []string{"Column1"}, Options: map[string]interface{}{"test1": 1}},
					},
				},
				&sql.SQLTable{
					Version: 1,
					Name:    "Table2",
					Columns: []sql.SQLTableColumn{
						{Name: "Column1", Type: "timestamptz", IsNullable: true, Position: 1},
						{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 2},
						{Name: "Column3", Type: "bigint", IsNullable: true, Position: 3},
						{Name: "Column4", Type: "boolean", IsNullable: true, Position: 4},
					},
					Constraints: []sql.SQLTableConstraint{
						{Type: sql.PrimaryKey, Name: "Column1_pk", Columns: []string{"Column1"}},
					},
					ForeignKeys: []sql.SQLTableForeignKey{
						{Name: "Table2_FK", Columns: []string{"Column3"}, ReferencedTable: "Table1", ReferencedColumns: []string{"Column1"}, UpdateAction: sql.NoAction, DeleteAction: sql.NoAction},
					},
					Indexes: []sql.SQLTableIndex{
						{Name: "Table3_IDX", Type: "btree", Columns: []string{"Column1"}, Options: map[string]interface{}{"test1": 1}},
					},
				},
			},
		},
	}
	recordBatchMap := map[string][]RecordBatchInfo{
		"Table1": {
			{
				batchName: "Batch1-1",
				chunks: []entities.SchemaRecordChunk{
					&sql.SQLRecordChunk{Content: []sql.SQLRecord{
						{Content: map[string]interface{}{"Column1": 1, "Column2": "test1"}},
						{Content: map[string]interface{}{"Column1": 2, "Column2": "test2"}},
					}},
					&sql.SQLRecordChunk{Content: []sql.SQLRecord{
						{Content: map[string]interface{}{"Column1": 3, "Column2": "test3"}},
						{Content: map[string]interface{}{"Column1": 4, "Column2": "test4"}},
					}},
				},
			}, {
				batchName: "Batch1-2",
				chunks: []entities.SchemaRecordChunk{
					&sql.SQLRecordChunk{Content: []sql.SQLRecord{
						{Content: map[string]interface{}{"Column1": 5, "Column2": "test5"}},
						{Content: map[string]interface{}{"Column1": 6, "Column2": "test6"}},
					}},
				},
			},
		},
		"Table2": {
			{
				batchName: "Batch2-1",
				chunks: []entities.SchemaRecordChunk{
					&sql.SQLRecordChunk{Content: []sql.SQLRecord{
						{Content: map[string]interface{}{"Column2": "test1", "Column3": 1}},
						{Content: map[string]interface{}{"Column2": "test2", "Column3": 2}},
						{Content: map[string]interface{}{"Column2": "test3", "Column3": 3}},
					}},
				},
				toDiffNames: []string{"diffs/Batch2-1"},
				toDiff: [][]entities.SchemaRecordChunk{
					{
						&sql.SQLRecordChunk{Content: []sql.SQLRecord{
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test1", "Column3": 1}},
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test3", "Column3": 3}},
							{Content: map[string]interface{}{"Column1": time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test4", "Column3": 4}},
						}},
					},
				},
			},
		},
		"Table3": {
			{
				batchName: "Batch3-1",
				chunks: []entities.SchemaRecordChunk{
					&sql.SQLRecordChunk{Content: []sql.SQLRecord{
						{Content: map[string]interface{}{"Column2": "test1", "Column3": 1}},
						{Content: map[string]interface{}{"Column2": "test2", "Column3": 2}},
					}},
					&sql.SQLRecordChunk{Content: []sql.SQLRecord{
						{Content: map[string]interface{}{"Column2": "test3", "Column3": 3}},
						{Content: map[string]interface{}{"Column2": "test4", "Column3": 4}},
					}},
				},
				toDiffNames: []string{"diffs/Batch3-1-1", "diffs/Batch3-1-2"},
				toDiff: [][]entities.SchemaRecordChunk{
					{
						&sql.SQLRecordChunk{Content: []sql.SQLRecord{
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test1", "Column3": 1}},
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test2", "Column3": 2}},
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test10", "Column3": 10}},
						}},
						&sql.SQLRecordChunk{Content: []sql.SQLRecord{
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test3", "Column3": 3}},
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test4", "Column3": 4}},
						}},
					}, {
						&sql.SQLRecordChunk{Content: []sql.SQLRecord{
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test2", "Column3": 2, "Column4": true}},
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test10", "Column3": 10, "Column4": false}},
						}},
						&sql.SQLRecordChunk{Content: []sql.SQLRecord{
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test3", "Column3": 3, "Column4": true}},
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test4", "Column3": 4, "Column4": true}},
							{Content: map[string]interface{}{"Column1": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), "Column2": "test11", "Column3": 11, "Column4": false}},
						}},
					},
				},
			},
		},
	}
	routinesMap := map[string]RoutineInfo{
		"Function1": {
			routine: &psql.PSQLFunction{Version: 1, Name: "Function1", Language: "sql", ReturnType: "integer", Tag: "$$", Definition: "RETURN 1"},
			toDiff: []entities.Routine{
				&psql.PSQLFunction{Version: 1, Name: "Function1", Language: "pgplpsql", Volatility: pointers.Ptr("STATIC"), Parameters: pointers.Ptr("p1 string, p2 string"), ReturnType: "integer", Tag: "$$", Definition: "RETURN LEN(p1) + LEN(p2)"},
			},
		},
		"Function2": {
			routine: &psql.PSQLFunction{Version: 1, Name: "Function2", Language: "sql", Parameters: pointers.Ptr("string, int"), ReturnType: "string", Tag: "$$", Definition: "RETURN $1"},
			toDiff: []entities.Routine{
				&psql.PSQLFunction{Version: 1, Name: "Function2", Language: "sql", Dependencies: []string{"Function1"}, Parameters: nil, ReturnType: "string", Tag: "$$", Definition: "RETURN string"},
				&psql.PSQLFunction{Version: 1, Name: "Function2", Language: "pgplpsql", Dependencies: []string{"Function1"}, Parameters: nil, ReturnType: "integer", Tag: "$$function$$", Definition: "RETURN Function1(test)"},
			},
		},
		"Procedure1": {
			routine: &psql.PSQLProcedure{Version: 1, Name: "Procedure1", Language: "sql", Tag: "$$", Definition: "LOOP 1 TO 1000"},
		},
		"Procedure2": {
			routine: &psql.PSQLProcedure{Version: 1, Name: "Procedure2", Language: "pgplpsql", Tag: "$$function$$", Definition: "LOOP 1 TO 50"},
			toDiff: []entities.Routine{
				&psql.PSQLProcedure{Version: 1, Name: "Procedure2", Language: "sql", Dependencies: []string{"Procedure1"}, Parameters: pointers.Ptr("string, string"), Tag: "$$", Definition: "DO $1 - $2 AND Procedure1()"},
			},
		},
		"Trigger1": {
			routine: &psql.PSQLTrigger{Version: 1, Name: "Trigger1", Definition: "STARTING TRIGGER"},
			toDiff: []entities.Routine{
				&psql.PSQLTrigger{Version: 1, Name: "Trigger1", Definition: "MIDDLE STEP TRIGGER"},
				&psql.PSQLTrigger{Version: 1, Name: "Trigger1", Definition: "ALMOST FINAL VERSION"},
				&psql.PSQLTrigger{Version: 1, Name: "Trigger1", Definition: "DO LOOP 1 TO 50 AND SUM(50, 20)"},
			},
		},
		"Trigger2": {
			routine: &psql.PSQLTrigger{Version: 1, Name: "Trigger2", Definition: "THIS IS THE SECOND"},
			toDiff: []entities.Routine{
				&psql.PSQLTrigger{Version: 1, Name: "Trigger2", Definition: "THIS IS THE SECOND TRIGGER DEFINITION"},
			},
		},
	}

	err = backupWriter.BeginSnapshot(&rollbackSnapshot)
	assert.Nil(t, err)

	err = backupWriter.SaveSchemaDependency(dependenciesMap["Sequence3"].dependency)
	assert.Nil(t, err)

	err = backupWriter.RollbackSnapshot()
	assert.Nil(t, err)

	err = backupWriter.BeginSnapshot(&snapshot)
	assert.Nil(t, err)

	for k, v := range dependenciesMap {
		err = backupWriter.SaveSchemaDependency(v.dependency)
		assert.Nil(t, err)

		snapshot.SchemaDependencies[k] = v.dependency.Hash()
		currentState := v.dependency

		for i, diffValue := range v.toDiff {
			var isDiff bool = false
			if i > 0 {
				isDiff = true
			}

			diff := diffValue.Diff(currentState, isDiff)
			err = backupWriter.SaveSchemaDependencyDiff(diff)
			assert.Nil(t, err)

			snapshot.SchemaDependencies[k] = fmt.Sprintf("diffs/%s", diff.Hash())
			currentState = diffValue
		}
	}

	for k, v := range schemasMap {
		err = backupWriter.SaveSchema(v.schema)
		assert.Nil(t, err)

		snapshot.Schemas[k] = v.schema.Hash()
		currentState := v.schema

		for i, diffValue := range v.toDiff {
			var isDiff bool = false
			if i > 0 {
				isDiff = true
			}

			diff := diffValue.Diff(currentState, isDiff)
			err = backupWriter.SaveSchemaDiff(diff)
			assert.Nil(t, err)

			snapshot.Schemas[k] = fmt.Sprintf("diffs/%s", diff.Hash())
			currentState = diffValue
		}
	}

	for k, v := range recordBatchMap {
		batches := []string{}
		for i, batch := range v {
			batchTempRef := fmt.Sprintf("temp-%s", batch.batchName)
			for _, chunk := range batch.chunks {
				err = backupWriter.SaveSchemaRecordChunk(batchTempRef, chunk)
				assert.Nil(t, err)
			}

			err = backupWriter.SaveSchemaRecordBatch(batchTempRef, batch.batchName)
			assert.Nil(t, err)
			batches = append(batches, batch.batchName)

			for j, diffValue := range batch.toDiff {
				for l, chunkDiff := range diffValue {
					if len(batch.chunks) > l {
						diff := chunkDiff.Diff(batch.chunks[l], false)

						err = backupWriter.SaveSchemaRecordChunkDiff(batch.batchName, batch.toDiffNames[j], diff)
						assert.Nil(t, err)

						batches[i] = batch.toDiffNames[j]
					}
				}
			}

			snapshot.Data[k] = entities.BackupSnapshotSchemaData{
				BatchSize: 100,
				ChunkSize: 100,
				Data:      batches,
			}
		}
	}

	for k, v := range routinesMap {
		err = backupWriter.SaveRoutine(v.routine)
		assert.Nil(t, err)

		snapshot.Routines[k] = v.routine.Hash()
		currentState := v.routine

		for i, diffValue := range v.toDiff {
			var isDiff bool = false
			if i > 0 {
				isDiff = true
			}

			diff := diffValue.Diff(currentState, isDiff)
			err = backupWriter.SaveRoutineDiff(diff)
			assert.Nil(t, err)

			snapshot.Routines[k] = fmt.Sprintf("diffs/%s", diff.Hash())
			currentState = diffValue
		}
	}

	err = backupWriter.CommitSnapshot(&metadata)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(metadata.Snapshots))

	// Reading snapshot
	readMetadata, err := backupReader.GetBackupMetadata()
	assert.Nil(t, err)
	assert.Equal(t, metadata, readMetadata)

	for _, snapshotInfo := range readMetadata.Snapshots {
		readSnapshot, err := backupReader.GetBackupSnapshot(snapshotInfo.SnapshotId)
		assert.Nil(t, err)
		assert.Equal(t, snapshot.Timestamp, readSnapshot.Timestamp)
		assert.Equal(t, snapshot.SnapshotId, readSnapshot.SnapshotId)

		for k, dependency := range readSnapshot.SchemaDependencies {
			readDependency, _, err := backupReader.GetSchemaDependency(dependency)
			assert.Nil(t, err)
			assert.Equal(t, expectedDependencies[k], readDependency)
		}

		for k, schema := range readSnapshot.Schemas {
			readSchema, _, err := backupReader.GetSchema(schema)
			assert.Nil(t, err)
			assert.Equal(t, expectedSchemas[k], readSchema)
		}

		for k, dataBatches := range readSnapshot.Data {
			for i, batch := range dataBatches.Data {
				chunkRefs, err := backupReader.GetSchemaRecordChunkRefsInBatch(batch)
				assert.Nil(t, err)
				assert.Equal(t, len(expectedRecordBatch[k][i].chunks), len(chunkRefs))

				for j, chunkRef := range chunkRefs {
					expectedRecordBatch[k][i].chunks[j].Hash()
					chunk, _, err := backupReader.GetSchemaRecordChunk(batch, chunkRef)
					chunk.Hash()

					assert.Nil(t, err)
					assert.Equal(t, expectedRecordBatch[k][i].chunks[j], chunk)
				}
			}
		}

		for k, routine := range readSnapshot.Routines {
			readRoutine, _, err := backupReader.GetRoutine(routine)
			assert.Nil(t, err)
			assert.Equal(t, expectedRoutines[k], readRoutine)
		}
	}

	// Deleting backup
	err = backupWriter.DeleteBackupStructure()
	assert.Nil(t, err)
}
