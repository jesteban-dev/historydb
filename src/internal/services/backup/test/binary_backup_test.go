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

var expectedDependencies map[string]entities.SchemaDependency = map[string]entities.SchemaDependency{
	"Sequence1": &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence1", Type: "integer", Start: 1, Min: 1, Max: 1000, Increment: 1, IsCycle: false, LastValue: 20, IsCalled: false},
	"Sequence2": &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence2", Type: "integer", Start: 20, Min: 2, Max: 100, Increment: 2, IsCycle: false, LastValue: 40, IsCalled: true},
	"Sequence3": &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 3, IsCycle: false, LastValue: 20, IsCalled: true},
}

var expectedSchemas map[string]entities.Schema = map[string]entities.Schema{
	"Table1": &sql.SQLTable{
		SchemaType: entities.SQLTable,
		Version:    "1",
		Name:       "Table1",
		Columns: []sql.SQLTableColumn{
			{Name: "Column1", Type: "integer", IsNullable: false, DefaultValue: pointers.Ptr("1"), Position: 1},
			{Name: "Column2", Type: "varying character(10)", IsNullable: true, Position: 2},
		},
		Constraints: []sql.SQLTableConstraint{
			{Type: sql.PrimaryKey, Name: "Column1_pk", Columns: []string{"Column1"}},
		},
	},
	"Table2": &sql.SQLTable{
		SchemaType: entities.SQLTable,
		Version:    "1",
		Name:       "Table2",
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
	"Table3": &sql.SQLTable{
		SchemaType: entities.SQLTable,
		Version:    "1",
		Name:       "Table2",
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
	}

	dependenciesMap := map[string]SchemaDependecyInfo{
		"Sequence1": {
			dependency: &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence1", Type: "integer", Start: 1, Min: 1, Max: 1000, Increment: 1, IsCycle: false, LastValue: 20, IsCalled: false},
		},
		"Sequence2": {
			dependency: &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence2", Type: "integer", Start: 20, Min: 2, Max: 100, Increment: 2, IsCycle: false, LastValue: 30, IsCalled: true},
			toDiff: []entities.SchemaDependency{
				&psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence2", Type: "integer", Start: 20, Min: 2, Max: 100, Increment: 2, IsCycle: false, LastValue: 40, IsCalled: true},
			},
		},
		"Sequence3": {
			dependency: &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 2, IsCycle: false, LastValue: 5, IsCalled: false},
			toDiff: []entities.SchemaDependency{
				&psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 3, IsCycle: false, LastValue: 10, IsCalled: true},
				&psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 3, IsCycle: false, LastValue: 20, IsCalled: true},
			},
		},
	}
	schemasMap := map[string]SchemaInfo{
		"Table1": {
			schema: &sql.SQLTable{
				SchemaType: entities.SQLTable,
				Version:    "1",
				Name:       "Table1",
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
				SchemaType: entities.SQLTable,
				Version:    "1",
				Name:       "Table2",
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
					SchemaType: entities.SQLTable,
					Version:    "1",
					Name:       "Table2",
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
				SchemaType: entities.SQLTable,
				Version:    "1",
				Name:       "Table2",
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
					SchemaType: entities.SQLTable,
					Version:    "1",
					Name:       "Table2",
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
					SchemaType: entities.SQLTable,
					Version:    "1",
					Name:       "Table2",
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
	}

	// Deleting backup
	err = backupWriter.DeleteBackupStructure()
	assert.Nil(t, err)
}
