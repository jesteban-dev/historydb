package test

import (
	"encoding/json"
	"fmt"
	"historydb/src/internal/entities"
	backup_services "historydb/src/internal/services/backup"
	"historydb/src/internal/services/backup/binary"
	"historydb/src/internal/services/entities/psql"
	"historydb/src/internal/services/entities/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type BinaryExpectedData struct {
	BackupExists bool                          `json:"backupExists"`
	Metadata     entities.BackupMetadata       `json:"metadata"`
	Snapshots    []entities.BackupSnapshot     `json:"snapshots"`
	Dependencies map[string]psql.PSQLSequence  `json:"dependencies"`
	Schemas      map[string]sql.SQLTable       `json:"schemas"`
	ChunkRefs    map[string][][]string         `json:"chunkRefs"`
	Data         map[string]ExpectedRecordData `json:"data"`
	Routines     map[string]ExpectedRoutine    `json:"routines"`
}

type ExpectedRecordData struct {
	Size      int                        `json:"size"`
	ChunkData [][]map[string]interface{} `json:"chunkData"`
}

type ExpectedRoutine struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type BatchChunkData struct {
	BatchRef string
	Chunks   []string
}

func TestBinaryBackupReader(t *testing.T) {
	testData, err := extractJSONTestData("data/binary_test_data.json")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, test := range testData {
		var expectedData BinaryExpectedData
		expectedDataBytes, _ := json.Marshal(test.ExpectedData)
		if err := json.Unmarshal(expectedDataBytes, &expectedData); err != nil {
			t.Fatal("could not decode expected data", err)
		}

		backupReader := binary.NewBinaryBackupReader(test.BackupPath)
		testCheckBackupExists(t, backupReader, expectedData.BackupExists)
		snapshot := testGetBackupMetadata(t, backupReader, expectedData.Metadata)
		testGetBackupSnapshot(t, backupReader, snapshot, expectedData.Snapshots[len(expectedData.Snapshots)-1])
		testGetSchemaDependency(t, backupReader, expectedData.Snapshots[len(expectedData.Snapshots)-1].SchemaDependencies, expectedData.Dependencies)
		testGetSchema(t, backupReader, expectedData.Snapshots[len(expectedData.Snapshots)-1].Schemas, expectedData.Schemas)
		dataChunks := testGetSchemaRecordChunkRefsInBatch(t, backupReader, expectedData.Snapshots[len(expectedData.Snapshots)-1].Data, expectedData.ChunkRefs)
		testGetSchemaRecordChunk(t, backupReader, dataChunks, expectedData.Data)
		testGetRoutine(t, backupReader, expectedData.Snapshots[len(expectedData.Snapshots)-1].Routines, expectedData.Routines)
	}
}

func testCheckBackupExists(t *testing.T, backupReader backup_services.BackupReader, expectedData bool) {
	assert.Equal(t, expectedData, backupReader.CheckBackupExists())
}

func testGetBackupMetadata(t *testing.T, backupReader backup_services.BackupReader, expectedData entities.BackupMetadata) string {
	backupMetadata, err := backupReader.GetBackupMetadata()
	assert.Nil(t, err)
	assert.NotNil(t, backupMetadata)

	assert.Equal(t, expectedData.Version, backupMetadata.Version)
	assert.Equal(t, expectedData.DatabaseEngine, backupMetadata.DatabaseEngine)
	assert.Equal(t, len(expectedData.Snapshots), len(backupMetadata.Snapshots))
	for i, snapshot := range backupMetadata.Snapshots {
		assert.Equal(t, expectedData.Snapshots[i].Timestamp.UTC(), snapshot.Timestamp.UTC())
		assert.Equal(t, expectedData.Snapshots[i].SnapshotId, snapshot.SnapshotId)
		assert.Equal(t, expectedData.Snapshots[i].Message, snapshot.Message)
	}

	return backupMetadata.Snapshots[len(backupMetadata.Snapshots)-1].SnapshotId
}

func testGetBackupSnapshot(t *testing.T, backupReader backup_services.BackupReader, snapshot string, expectedData entities.BackupSnapshot) {
	backupSnapshot, err := backupReader.GetBackupSnapshot(snapshot)
	assert.Nil(t, err)
	assert.NotNil(t, backupSnapshot)

	assert.Equal(t, expectedData.Version, backupSnapshot.Version)
	assert.Equal(t, expectedData.Timestamp.UTC(), backupSnapshot.Timestamp.UTC())
	assert.Equal(t, expectedData.SnapshotId, backupSnapshot.SnapshotId)
	assert.Equal(t, expectedData.Message, backupSnapshot.Message)
	assert.Equal(t, expectedData.SchemaDependencies, backupSnapshot.SchemaDependencies)
	assert.Equal(t, expectedData.Schemas, backupSnapshot.Schemas)
	assert.Equal(t, expectedData.Data, backupSnapshot.Data)
	assert.Equal(t, expectedData.Routines, backupSnapshot.Routines)
}

func testGetSchemaDependency(t *testing.T, backupReader backup_services.BackupReader, dependencies map[string]string, expectedData map[string]psql.PSQLSequence) {
	for key, ref := range dependencies {
		dependency, _, err := backupReader.GetSchemaDependency(ref)
		assert.Nil(t, err)
		assert.NotNil(t, dependency)
		assert.Equal(t, expectedData[key], *dependency.(*psql.PSQLSequence))
	}
}

func testGetSchema(t *testing.T, backupReader backup_services.BackupReader, schemas map[string]string, expectedData map[string]sql.SQLTable) {
	for key, ref := range schemas {
		schema, _, err := backupReader.GetSchema(ref)
		assert.Nil(t, err)
		assert.NotNil(t, schema)
		assert.Equal(t, expectedData[key], *schema.(*sql.SQLTable))
	}
}

func testGetSchemaRecordChunkRefsInBatch(t *testing.T, backupReader backup_services.BackupReader, batches map[string]entities.BackupSnapshotSchemaData, expectedData map[string][][]string) map[string][]BatchChunkData {
	dataChunks := make(map[string][]BatchChunkData)

	for key, value := range batches {
		batches := make([]BatchChunkData, 0, len(batches))

		for i, batch := range value.Data {
			chunkRefs, err := backupReader.GetSchemaRecordChunkRefsInBatch(batch)
			assert.Nil(t, err)
			assert.NotNil(t, chunkRefs)
			assert.Equal(t, expectedData[key][i], chunkRefs)

			batches = append(batches, BatchChunkData{BatchRef: batch, Chunks: chunkRefs})
		}

		dataChunks[key] = batches
	}

	return dataChunks
}

func testGetSchemaRecordChunk(t *testing.T, backupReader backup_services.BackupReader, dataChunks map[string][]BatchChunkData, expectedData map[string]ExpectedRecordData) {
	for key, value := range dataChunks {
		for _, batch := range value {
			for i, chunk := range batch.Chunks {
				recordChunk, _, err := backupReader.GetSchemaRecordChunk(batch.BatchRef, chunk)
				assert.Nil(t, err)
				assert.NotNil(t, recordChunk)

				j := 0
				for _, expectedRecord := range expectedData[key].ChunkData[i] {
					assert.Equal(t, normalize(expectedRecord), normalize(recordChunk.(*sql.SQLRecordChunk).Content[j].Content))
					j += expectedData[key].Size
				}
			}
		}
	}
}

func testGetRoutine(t *testing.T, backupReader backup_services.BackupReader, routineRefs map[string]string, expectedData map[string]ExpectedRoutine) {
	for key, ref := range routineRefs {
		routine, _, err := backupReader.GetRoutine(ref)
		assert.Nil(t, err)
		assert.NotNil(t, routine)

		switch expectedData[key].Type {
		case "PSQLTrigger":
			var expectedRoutine psql.PSQLTrigger
			expectedRoutineBytes, _ := json.Marshal(expectedData[key].Data)
			json.Unmarshal(expectedRoutineBytes, &expectedRoutine)
			assert.Equal(t, &expectedRoutine, routine)
		case "PSQLProcedure":
			var expectedRoutine psql.PSQLProcedure
			expectedRoutineBytes, _ := json.Marshal(expectedData[key].Data)
			json.Unmarshal(expectedRoutineBytes, &expectedRoutine)
			assert.Equal(t, &expectedRoutine, routine)
		case "PSQLFunction":
			var expectedRoutine psql.PSQLFunction
			expectedRoutineBytes, _ := json.Marshal(expectedData[key].Data)
			json.Unmarshal(expectedRoutineBytes, &expectedRoutine)
			assert.Equal(t, &expectedRoutine, routine)
		}
	}
}
