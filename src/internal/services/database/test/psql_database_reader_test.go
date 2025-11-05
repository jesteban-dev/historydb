package test

import (
	"context"
	"database/sql"
	"fmt"
	"historydb/src/internal/entities"
	services "historydb/src/internal/services/database"
	"historydb/src/internal/services/database/psql"
	psql_entities "historydb/src/internal/services/entities/psql"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PSQLTestContent struct {
	DBName    string
	IsEmpty   bool
	Sequences []psql_entities.PSQLSequence
}

var imageData = map[string]PSQLTestContent{
	"postgres:latest": {
		DBName:  "test",
		IsEmpty: true,
	},
	"aa8y/postgres-dataset:world": {
		DBName:  "world",
		IsEmpty: false,
	},
	"aa8y/postgres-dataset:dellstore": {
		DBName:  "dellstore",
		IsEmpty: false,
		Sequences: []psql_entities.PSQLSequence{
			{DependencyType: entities.PSQLSequence, Version: psql_entities.CURRENT_VERSION, Name: "public.categories_category_seq", Type: "integer", Start: 1, Min: 1, Max: 2147483647, Increment: 1, IsCycle: false, LastValue: 16, IsCalled: true},
			{DependencyType: entities.PSQLSequence, Version: psql_entities.CURRENT_VERSION, Name: "public.customers_customerid_seq", Type: "integer", Start: 1, Min: 1, Max: 2147483647, Increment: 1, IsCycle: false, LastValue: 20000, IsCalled: true},
			{DependencyType: entities.PSQLSequence, Version: psql_entities.CURRENT_VERSION, Name: "public.orders_orderid_seq", Type: "integer", Start: 1, Min: 1, Max: 2147483647, Increment: 1, IsCycle: false, LastValue: 12000, IsCalled: true},
			{DependencyType: entities.PSQLSequence, Version: psql_entities.CURRENT_VERSION, Name: "public.products_prod_id_seq", Type: "integer", Start: 1, Min: 1, Max: 2147483647, Increment: 1, IsCycle: false, LastValue: 10000, IsCalled: true},
		},
	},
}

func TestPSQLReader(t *testing.T) {
	for image, data := range imageData {
		db, cleanup := setupPSQLContainer(t, image, data.DBName)

		dbReader := psql.NewPSQLDatabaseReader(db)
		testCheckDBIsEmpty(t, dbReader, data.IsEmpty)
		testListSchemaDependencies(t, dbReader, data.Sequences)

		cleanup()
	}
}

func setupPSQLContainer(t *testing.T, image string, dbName string) (*sql.DB, func()) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test"},
		WaitingFor:   wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
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
		log.Fatalf("failed to connect to postgres database: %v", err)
	}

	cleanup := func() {
		container.Terminate(ctx)
	}

	return db, cleanup
}

func testCheckDBIsEmpty(t *testing.T, dbReader services.DatabaseReader, expectedData bool) {
	isEmpty, err := dbReader.CheckDBIsEmpty()
	assert.Nil(t, err)
	assert.Equal(t, expectedData, isEmpty)
}

func testListSchemaDependencies(t *testing.T, dbReader services.DatabaseReader, expectedData []psql_entities.PSQLSequence) {
	schemaDependencies, err := dbReader.ListSchemaDependencies()
	assert.Nil(t, err)
	assert.Equal(t, len(expectedData), len(schemaDependencies))

	for i, dependency := range schemaDependencies {
		assert.Equal(t, expectedData[i], *dependency.(*psql_entities.PSQLSequence))
	}
}
