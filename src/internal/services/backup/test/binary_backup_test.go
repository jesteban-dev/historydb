package test

import (
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services/backup/binary"
	"historydb/src/internal/services/entities/psql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type SchemaDependecyInfo struct {
	dependency entities.SchemaDependency
	toDiff     entities.SchemaDependency
}

var expectedDependencies map[string]entities.SchemaDependency = map[string]entities.SchemaDependency{
	"Sequence1": &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence1", Type: "integer", Start: 1, Min: 1, Max: 1000, Increment: 1, IsCycle: false, LastValue: 20, IsCalled: false},
	"Sequence2": &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence2", Type: "integer", Start: 20, Min: 2, Max: 100, Increment: 2, IsCycle: false, LastValue: 30, IsCalled: true},
	"Sequence3": &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 3, IsCycle: false, LastValue: 10, IsCalled: true},
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
	}

	dependenciesMap := map[string]SchemaDependecyInfo{
		"Sequence1": {
			dependency: &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence1", Type: "integer", Start: 1, Min: 1, Max: 1000, Increment: 1, IsCycle: false, LastValue: 20, IsCalled: false},
		},
		"Sequence2": {
			dependency: &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence2", Type: "integer", Start: 20, Min: 2, Max: 100, Increment: 2, IsCycle: false, LastValue: 30, IsCalled: true},
		},
		"Sequence3": {
			dependency: &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 2, IsCycle: false, LastValue: 5, IsCalled: false},
			toDiff:     &psql.PSQLSequence{DependencyType: entities.PSQLSequence, Version: "1", Name: "Sequence3", Type: "integer", Start: 1, Min: 2, Max: 10000, Increment: 3, IsCycle: false, LastValue: 10, IsCalled: true},
		},
	}

	err = backupWriter.BeginSnapshot(&rollbackSnapshot)
	assert.Nil(t, err)

	err = backupWriter.SaveSchemaDependency(dependenciesMap["Sequence1"].dependency)
	assert.Nil(t, err)

	err = backupWriter.RollbackSnapshot()
	assert.Nil(t, err)

	err = backupWriter.BeginSnapshot(&snapshot)
	assert.Nil(t, err)

	for k, v := range dependenciesMap {
		err = backupWriter.SaveSchemaDependency(v.dependency)
		assert.Nil(t, err)

		snapshot.SchemaDependencies[k] = v.dependency.Hash()

		if v.toDiff != nil {
			diff := v.toDiff.Diff(v.dependency)
			err = backupWriter.SaveSchemaDependencyDiff(diff)
			assert.Nil(t, err)

			snapshot.SchemaDependencies[k] = fmt.Sprintf("diffs/%s", diff.Hash())
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
			readDependency, err := backupReader.GetSchemaDependency(dependency)
			assert.Nil(t, err)
			assert.Equal(t, expectedDependencies[k], readDependency)
		}
	}

	// Deleting backup
	err = backupWriter.DeleteBackupStructure()
	assert.Nil(t, err)
}
