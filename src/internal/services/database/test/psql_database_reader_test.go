package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"historydb/src/internal/entities"
	services "historydb/src/internal/services/database"
	"historydb/src/internal/services/database/psql"
	psql_entities "historydb/src/internal/services/entities/psql"
	sql_entities "historydb/src/internal/services/entities/sql"
	"historydb/src/internal/utils/types"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PSQLExpectedData struct {
	IsEmpty      bool                         `json:"isEmpty"`
	Sequences    []psql_entities.PSQLSequence `json:"sequences,omitempty"`
	Tables       []sql_entities.SQLTable      `json:"tables"`
	TableContent map[string]PSQLTableContent  `json:"tableContent"`
	Routines     []interface{}                `json:"routines"`
}

type PSQLTableContent struct {
	Length     int                      `json:"length"`
	MaxRowSize int                      `json:"maxRowSize"`
	ChunkSize  int                      `json:"chunkSize"`
	Records    []map[string]interface{} `json:"records"`
}

type PSQLRoutineType struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

func TestPSQLReader(t *testing.T) {
	testData, err := extractJSONTestData("data/psql_test_data.json")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, test := range testData {
		db, cleanup, err := setupPSQLTestContainer(t, test.Image, test.InitScript, test.DBName)
		if err == nil {
			var expectedData PSQLExpectedData
			expectedDataBytes, _ := json.Marshal(test.ExpectedData)
			if err := json.Unmarshal(expectedDataBytes, &expectedData); err != nil {
				t.Fatal("could not decode expected data", err)
			}

			dbReader := psql.NewPSQLDatabaseReader(db)
			testCheckDBIsEmpty(t, test.Name, dbReader, expectedData.IsEmpty)
			testListSchemaDependencies(t, test.Name, dbReader, expectedData.Sequences)
			testListSchemaNames(t, test.Name, dbReader, expectedData.Tables)
			testGetSchemaDefinition(t, test.Name, dbReader, expectedData.Tables)
			testGetSchemaRecordMetadata(t, test.Name, dbReader, expectedData.TableContent)
			testGetSchemaRecordChunk(t, test.Name, dbReader, expectedData.TableContent)

			expectedRoutines := make([]entities.Routine, 0, len(expectedData.Routines))
			for _, routine := range expectedData.Routines {
				var routineInfo PSQLRoutineType
				routineInfoBytes, _ := json.Marshal(routine)
				if err := json.Unmarshal(routineInfoBytes, &routineInfo); err != nil {
					t.Fatal("could not decode routine", err)
				}

				routineDataBytes, _ := json.Marshal(routineInfo.Data)
				switch routineInfo.Type {
				case "function":
					var routineData psql_entities.PSQLFunction
					if err := json.Unmarshal(routineDataBytes, &routineData); err != nil {
						t.Fatal("could not decode routine", err)
					}
					expectedRoutines = append(expectedRoutines, &routineData)
				case "procedure":
					var routineData psql_entities.PSQLProcedure
					if err := json.Unmarshal(routineDataBytes, &routineData); err != nil {
						t.Fatal("could not decode routine", err)
					}
					expectedRoutines = append(expectedRoutines, &routineData)
				case "trigger":
					var routineData psql_entities.PSQLTrigger
					if err := json.Unmarshal(routineDataBytes, &routineData); err != nil {
						t.Fatal("could not decode routine", err)
					}
					expectedRoutines = append(expectedRoutines, &routineData)
				}
			}
			testListRoutines(t, test.Name, dbReader, expectedRoutines)

			cleanup()
		} else {
			fmt.Println(err)
		}
	}
}

func setupPSQLTestContainer(t *testing.T, image string, initScript *string, dbName string) (*sql.DB, func(), error) {
	ctx := context.Background()

	envs := map[string]string{"POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test"}
	if strings.HasPrefix(image, "postgres") {
		envs["POSTGRES_DB"] = dbName
	}
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"5432/tcp"},
		Env:          envs,
		WaitingFor:   wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}
	if initScript != nil {
		if _, err := os.Stat(*initScript); err != nil {
			return nil, nil, fmt.Errorf("init script file not found")
		}

		req.Files = []testcontainers.ContainerFile{{
			HostFilePath:      *initScript,
			ContainerFilePath: "/docker-entrypoint-initdb.d/init.sql",
			FileMode:          0o644,
		}}
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("could not generate testcontainer for: %s | %v", image, err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("could not get testcontainer host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("could not get testcontainer mapped port: %v", err)
	}

	dbUrl := fmt.Sprintf("postgres://test:test@%s:%s/%s?sslmode=disable", host, port.Port(), dbName)
	dsn, err := parseDatabaseURL(dbUrl)
	if err != nil {
		t.Fatalf("could not parse db url: %v", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to connect to postgres database: %v", err)
	}

	cleanup := func() {
		container.Terminate(ctx)
	}

	return db, cleanup, nil
}

func testCheckDBIsEmpty(t *testing.T, testName string, dbReader services.DatabaseReader, expectedData bool) {
	isEmpty, err := dbReader.CheckDBIsEmpty()
	assert.Nil(t, err, fmt.Sprintf("CheckDBIsEmpty - Test: %s", testName))
	assert.Equal(t, expectedData, isEmpty, fmt.Sprintf("CheckDBIsEmpty - Test: %s", testName))
}

func testListSchemaDependencies(t *testing.T, testName string, dbReader services.DatabaseReader, expectedData []psql_entities.PSQLSequence) {
	schemaDependencies, err := dbReader.ListSchemaDependencies()
	assert.Nil(t, err, fmt.Sprintf("ListSchemaDependencies - Test: %s", testName))
	if assert.Equal(t, len(expectedData), len(schemaDependencies), fmt.Sprintf("ListSchemaDependencies - Test: %s", testName)) {
		for idx, dependency := range schemaDependencies {
			assert.Equal(t, expectedData[idx], *dependency.(*psql_entities.PSQLSequence), fmt.Sprintf("ListSchemaDependencies - Test: %s", testName))
		}
	}
}

func testListSchemaNames(t *testing.T, testName string, dbReader services.DatabaseReader, expectedData []sql_entities.SQLTable) {
	schemaNames, err := dbReader.ListSchemaNames()
	assert.Nil(t, err, fmt.Sprintf("ListSchemaNames - Test: %s", testName))
	if assert.Equal(t, len(expectedData), len(schemaNames), fmt.Sprintf("ListSchemaNames - Test: %s", testName)) {
		for idx, tableName := range schemaNames {
			assert.Equal(t, expectedData[idx].Name, tableName, fmt.Sprintf("ListSchemaNames - Test: %s", testName))
		}
	}
}

func testGetSchemaDefinition(t *testing.T, testName string, dbReader services.DatabaseReader, expectedData []sql_entities.SQLTable) {
	for _, expectedTable := range expectedData {
		schema, err := dbReader.GetSchemaDefinition(expectedTable.Name)
		assert.Nil(t, err, fmt.Sprintf("GetSchemaDefinition - Test: %s", testName))
		assert.NotNil(t, schema, fmt.Sprintf("GetSchemaDefinition - Test: %s", testName))

		if schema != nil {
			table := schema.(*sql_entities.SQLTable)
			assert.Equal(t, expectedTable.Name, table.Name, fmt.Sprintf("GetSchemaDefinition - Test: %v", testName))
			assert.Equal(t, types.NormalizeSlice(expectedTable.Columns), types.NormalizeSlice(table.Columns), fmt.Sprintf("GetSchemaDefinition - Test: %v", testName))
			assert.Equal(t, types.NormalizeSlice(expectedTable.Constraints), types.NormalizeSlice(table.Constraints), fmt.Sprintf("GetSchemaDefinition - Test: %v", testName))
			assert.Equal(t, types.NormalizeSlice(expectedTable.ForeignKeys), types.NormalizeSlice(table.ForeignKeys), fmt.Sprintf("GetSchemaDefinition - Test: %v", testName))
			assert.Equal(t, types.NormalizeSlice(expectedTable.Indexes), types.NormalizeSlice(table.Indexes), fmt.Sprintf("GetSchemaDefinition - Test: %v", testName))
		}
	}
}

func testGetSchemaRecordMetadata(t *testing.T, testName string, dbReader services.DatabaseReader, expectedData map[string]PSQLTableContent) {
	for tableName, expectedTableData := range expectedData {
		metadata, err := dbReader.GetSchemaRecordMetadata(tableName)
		assert.Nil(t, err, fmt.Sprintf("GetSchemaRecordMetadata - Test: %s", testName))
		assert.Equal(t, expectedTableData.Length, metadata.Count, fmt.Sprintf("GetSchemaRecordMetadata - Test: %s", testName))
		assert.Equal(t, expectedTableData.MaxRowSize, metadata.MaxRecordSize, fmt.Sprintf("GetSchemaRecordMetadata - Test: %s", testName))
	}
}

func testGetSchemaRecordChunk(t *testing.T, testName string, dbReader services.DatabaseReader, expectedData map[string]PSQLTableContent) {
	for tableName, expectedTableData := range expectedData {
		schema, err := dbReader.GetSchemaDefinition(tableName)
		assert.Nil(t, err, fmt.Sprintf("GetSchemaRecordChunk - GetSchemaDefinition - Test: %s", testName))
		assert.NotNil(t, schema, fmt.Sprintf("GetSchemaRecordChunk - GetSchemaDefinition - Test: %s", testName))

		var cursor interface{} = nil
		expectedRecordIndex := 0
		for {
			chunk, nextCursor, err := dbReader.GetSchemaRecordChunk(schema, int64(expectedTableData.ChunkSize), cursor)
			assert.Nil(t, err, fmt.Sprintf("GetSchemaRecordChunk - Test: %s", testName))

			chunkRecord := chunk.(*sql_entities.SQLRecordChunk)
			if len(chunkRecord.Content) == 0 {
				break
			} else {
				assert.LessOrEqual(t, len(chunkRecord.Content), expectedTableData.ChunkSize, fmt.Sprintf("GetSchemaRecordChunk - Test: %s", testName))

				for k, v := range chunkRecord.Content[0].Content {
					switch a := v.(type) {
					case time.Time:
						var recordValue interface{}
						for key, value := range expectedTableData.Records[expectedRecordIndex] {
							if key == k {
								recordValue = value
							}
						}
						e, err := time.Parse(time.RFC3339, recordValue.(string))
						assert.Nil(t, err, fmt.Sprintf("GetSchemaRecordChunk - Test: %v", testName))
						assert.Equal(t, e.Unix(), a.Unix(), fmt.Sprintf("GetSchemaRecordChunk - Test: %v", testName))
					case int64:
						var recordValue interface{}
						for key, value := range expectedTableData.Records[expectedRecordIndex] {
							if key == k {
								recordValue = value
							}
						}
						assert.Equal(t, v, int64(recordValue.(float64)), fmt.Sprintf("GetSchemaRecordChunk - Test: %v", testName))
					default:
						var recordValue interface{}
						for key, value := range expectedTableData.Records[expectedRecordIndex] {
							if key == k {
								recordValue = value
							}
						}
						assert.Equal(t, v, recordValue, fmt.Sprintf("GetSchemaRecordChunk - Test: %v", testName))
					}
				}
				expectedRecordIndex++
			}

			cursor = nextCursor
		}
	}
}

func testListRoutines(t *testing.T, testName string, dbReader services.DatabaseReader, expectedRoutines []entities.Routine) {
	routines, err := dbReader.ListRoutines()
	assert.Nil(t, err, fmt.Sprintf("ListRoutines - Test: %s", testName))
	if assert.Equal(t, len(expectedRoutines), len(routines), fmt.Sprintf("ListRoutines - Test: %s", testName)) {
		for i, routine := range routines {
			assert.Equal(t, expectedRoutines[i], routine, fmt.Sprintf("ListRoutines - Test: %s", testName))
		}
	}
}
