package test

import (
	"context"
	"database/sql"
	"fmt"
	"historydb/src/internal/entities"
	services "historydb/src/internal/services/database"
	"historydb/src/internal/services/database/psql"
	psql_entities "historydb/src/internal/services/entities/psql"
	sql_entities "historydb/src/internal/services/entities/sql"
	"historydb/src/internal/utils/pointers"
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
	Tables    []sql_entities.SQLTable
}

var imageData = map[string]PSQLTestContent{
	"postgres:latest": {
		DBName:  "test",
		IsEmpty: true,
	},
	"aa8y/postgres-dataset:world": {
		DBName:  "world",
		IsEmpty: false,
		Tables: []sql_entities.SQLTable{
			{
				Name: "public.city",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "name", Type: "text", IsNullable: false, Position: 2},
					{Name: "countrycode", Type: "character(3)", IsNullable: false, Position: 3},
					{Name: "district", Type: "text", IsNullable: false, Position: 4},
					{Name: "population", Type: "integer", IsNullable: false, Position: 5},
				},
				Constraints: []sql_entities.SQLTableConstraint{
					{Type: sql_entities.PrimaryKey, Name: "city_pkey", Columns: []string{"id"}},
				},
				ForeignKeys: []sql_entities.SQLTableForeignKey{},
				Indexes:     []sql_entities.SQLTableIndex{},
			}, {
				Name: "public.country",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "code", Type: "character(3)", IsNullable: false, Position: 1},
					{Name: "name", Type: "text", IsNullable: false, Position: 2},
					{Name: "continent", Type: "text", IsNullable: false, Position: 3},
					{Name: "region", Type: "text", IsNullable: false, Position: 4},
					{Name: "surfacearea", Type: "real", IsNullable: false, Position: 5},
					{Name: "indepyear", Type: "smallint", IsNullable: true, Position: 6},
					{Name: "population", Type: "integer", IsNullable: false, Position: 7},
					{Name: "lifeexpectancy", Type: "real", IsNullable: true, Position: 8},
					{Name: "gnp", Type: "numeric(10,2)", IsNullable: true, Position: 9},
					{Name: "gnpold", Type: "numeric(10,2)", IsNullable: true, Position: 10},
					{Name: "localname", Type: "text", IsNullable: false, Position: 11},
					{Name: "governmentform", Type: "text", IsNullable: false, Position: 12},
					{Name: "headofstate", Type: "text", IsNullable: true, Position: 13},
					{Name: "capital", Type: "integer", IsNullable: true, Position: 14},
					{Name: "code2", Type: "character(2)", IsNullable: false, Position: 15},
				},
				Constraints: []sql_entities.SQLTableConstraint{
					{Type: sql_entities.Check, Name: "country_continent_check", Definition: pointers.Ptr("((continent = 'Asia'::text) OR (continent = 'Europe'::text) OR (continent = 'North America'::text) OR (continent = 'Africa'::text) OR (continent = 'Oceania'::text) OR (continent = 'Antarctica'::text) OR (continent = 'South America'::text))")},
					{Type: sql_entities.PrimaryKey, Name: "country_pkey", Columns: []string{"code"}},
				},
				ForeignKeys: []sql_entities.SQLTableForeignKey{
					{Name: "country_capital_fkey", Columns: []string{"capital"}, ReferencedTable: "public.city", ReferencedColumns: []string{"id"}, UpdateAction: sql_entities.NoAction, DeleteAction: sql_entities.NoAction},
				},
				Indexes: []sql_entities.SQLTableIndex{},
			}, {
				Name: "public.countrylanguage",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "countrycode", Type: "character(3)", IsNullable: false, Position: 1},
					{Name: "language", Type: "text", IsNullable: false, Position: 2},
					{Name: "isofficial", Type: "boolean", IsNullable: false, Position: 3},
					{Name: "percentage", Type: "real", IsNullable: false, Position: 4},
				},
				Constraints: []sql_entities.SQLTableConstraint{
					{Type: sql_entities.PrimaryKey, Name: "countrylanguage_pkey", Columns: []string{"countrycode", "language"}},
				},
				ForeignKeys: []sql_entities.SQLTableForeignKey{
					{Name: "countrylanguage_countrycode_fkey", Columns: []string{"countrycode"}, ReferencedTable: "public.country", ReferencedColumns: []string{"code"}, UpdateAction: sql_entities.NoAction, DeleteAction: sql_entities.NoAction},
				},
				Indexes: []sql_entities.SQLTableIndex{},
			},
		},
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
		Tables: []sql_entities.SQLTable{
			{
				Name: "public.categories",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "category", Type: "integer", IsNullable: false, DefaultValue: pointers.Ptr("nextval('categories_category_seq'::regclass)"), Position: 1},
					{Name: "categoryname", Type: "character varying(50)", IsNullable: false, Position: 2},
				},
				Constraints: []sql_entities.SQLTableConstraint{
					{Type: sql_entities.PrimaryKey, Name: "categories_pkey", Columns: []string{"category"}},
				},
				ForeignKeys: []sql_entities.SQLTableForeignKey{},
				Indexes:     []sql_entities.SQLTableIndex{},
			}, {
				Name: "public.cust_hist",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "customerid", Type: "integer", IsNullable: false, Position: 1},
					{Name: "orderid", Type: "integer", IsNullable: false, Position: 2},
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 3},
				},
				Constraints: []sql_entities.SQLTableConstraint{},
				ForeignKeys: []sql_entities.SQLTableForeignKey{
					{Name: "fk_cust_hist_customerid", Columns: []string{"customerid"}, ReferencedTable: "public.customers", ReferencedColumns: []string{"customerid"}, UpdateAction: sql_entities.NoAction, DeleteAction: sql_entities.Cascade},
				},
				Indexes: []sql_entities.SQLTableIndex{
					{Name: "ix_cust_hist_customerid", Type: "btree", Columns: []string{"customerid"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				Name: "public.customers",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "customerid", Type: "integer", IsNullable: false, DefaultValue: pointers.Ptr("nextval('customers_customerid_seq'::regclass)"), Position: 1},
					{Name: "firstname", Type: "character varying(50)", IsNullable: false, Position: 2},
					{Name: "lastname", Type: "character varying(50)", IsNullable: false, Position: 3},
					{Name: "address1", Type: "character varying(50)", IsNullable: false, Position: 4},
					{Name: "address2", Type: "character varying(50)", IsNullable: true, Position: 5},
					{Name: "city", Type: "character varying(50)", IsNullable: false, Position: 6},
					{Name: "state", Type: "character varying(50)", IsNullable: true, Position: 7},
					{Name: "zip", Type: "integer", IsNullable: true, Position: 8},
					{Name: "country", Type: "character varying(50)", IsNullable: false, Position: 9},
					{Name: "region", Type: "smallint", IsNullable: false, Position: 10},
					{Name: "email", Type: "character varying(50)", IsNullable: true, Position: 11},
					{Name: "phone", Type: "character varying(50)", IsNullable: true, Position: 12},
					{Name: "creditcardtype", Type: "integer", IsNullable: false, Position: 13},
					{Name: "creditcard", Type: "character varying(50)", IsNullable: false, Position: 14},
					{Name: "creditcardexpiration", Type: "character varying(50)", IsNullable: false, Position: 15},
					{Name: "username", Type: "character varying(50)", IsNullable: false, Position: 16},
					{Name: "password", Type: "character varying(50)", IsNullable: false, Position: 17},
					{Name: "age", Type: "smallint", IsNullable: true, Position: 18},
					{Name: "income", Type: "integer", IsNullable: true, Position: 19},
					{Name: "gender", Type: "character varying(1)", IsNullable: true, Position: 20},
				},
				Constraints: []sql_entities.SQLTableConstraint{
					{Type: sql_entities.PrimaryKey, Name: "customers_pkey", Columns: []string{"customerid"}},
				},
				ForeignKeys: []sql_entities.SQLTableForeignKey{},
				Indexes: []sql_entities.SQLTableIndex{
					{Name: "ix_cust_username", Type: "btree", Columns: []string{"username"}, Options: map[string]interface{}{"isUnique": true}},
				},
			}, {
				Name: "public.inventory",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "quan_in_stock", Type: "integer", IsNullable: false, Position: 2},
					{Name: "sales", Type: "integer", IsNullable: false, Position: 3},
				},
				Constraints: []sql_entities.SQLTableConstraint{
					{Type: sql_entities.PrimaryKey, Name: "inventory_pkey", Columns: []string{"prod_id"}},
				},
				ForeignKeys: []sql_entities.SQLTableForeignKey{},
				Indexes:     []sql_entities.SQLTableIndex{},
			}, {
				Name: "public.orderlines",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "orderlineid", Type: "integer", IsNullable: false, Position: 1},
					{Name: "orderid", Type: "integer", IsNullable: false, Position: 2},
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 3},
					{Name: "quantity", Type: "smallint", IsNullable: false, Position: 4},
					{Name: "orderdate", Type: "date", IsNullable: false, Position: 5},
				},
				Constraints: []sql_entities.SQLTableConstraint{},
				ForeignKeys: []sql_entities.SQLTableForeignKey{
					{Name: "fk_orderid", Columns: []string{"orderid"}, ReferencedTable: "public.orders", ReferencedColumns: []string{"orderid"}, UpdateAction: sql_entities.NoAction, DeleteAction: sql_entities.Cascade},
				},
				Indexes: []sql_entities.SQLTableIndex{
					{Name: "ix_orderlines_orderid", Type: "btree", Columns: []string{"orderid", "orderlineid"}, Options: map[string]interface{}{"isUnique": true}},
				},
			}, {
				Name: "public.orders",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "orderid", Type: "integer", IsNullable: false, DefaultValue: pointers.Ptr("nextval('orders_orderid_seq'::regclass)"), Position: 1},
					{Name: "orderdate", Type: "date", IsNullable: false, Position: 2},
					{Name: "customerid", Type: "integer", IsNullable: true, Position: 3},
					{Name: "netamount", Type: "numeric(12,2)", IsNullable: false, Position: 4},
					{Name: "tax", Type: "numeric(12,2)", IsNullable: false, Position: 5},
					{Name: "totalamount", Type: "numeric(12,2)", IsNullable: false, Position: 6},
				},
				Constraints: []sql_entities.SQLTableConstraint{
					{Type: sql_entities.PrimaryKey, Name: "orders_pkey", Columns: []string{"orderid"}},
				},
				ForeignKeys: []sql_entities.SQLTableForeignKey{
					{Name: "fk_customerid", Columns: []string{"customerid"}, ReferencedTable: "public.customers", ReferencedColumns: []string{"customerid"}, UpdateAction: sql_entities.NoAction, DeleteAction: sql_entities.SetNull},
				},
				Indexes: []sql_entities.SQLTableIndex{
					{Name: "ix_order_custid", Type: "btree", Columns: []string{"customerid"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				Name: "public.products",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, DefaultValue: pointers.Ptr("nextval('products_prod_id_seq'::regclass)"), Position: 1},
					{Name: "category", Type: "integer", IsNullable: false, Position: 2},
					{Name: "title", Type: "character varying(50)", IsNullable: false, Position: 3},
					{Name: "actor", Type: "character varying(50)", IsNullable: false, Position: 4},
					{Name: "price", Type: "numeric(12,2)", IsNullable: false, Position: 5},
					{Name: "special", Type: "smallint", IsNullable: true, Position: 6},
					{Name: "common_prod_id", Type: "integer", IsNullable: false, Position: 7},
				},
				Constraints: []sql_entities.SQLTableConstraint{
					{Type: sql_entities.PrimaryKey, Name: "products_pkey", Columns: []string{"prod_id"}},
				},
				ForeignKeys: []sql_entities.SQLTableForeignKey{},
				Indexes: []sql_entities.SQLTableIndex{
					{Name: "ix_prod_category", Type: "btree", Columns: []string{"category"}, Options: map[string]interface{}{"isUnique": false}},
					{Name: "ix_prod_special", Type: "btree", Columns: []string{"special"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				Name: "public.reorder",
				Columns: []sql_entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "date_low", Type: "date", IsNullable: false, Position: 2},
					{Name: "quan_low", Type: "integer", IsNullable: false, Position: 3},
					{Name: "date_reordered", Type: "date", IsNullable: true, Position: 4},
					{Name: "quan_reordered", Type: "integer", IsNullable: true, Position: 5},
					{Name: "date_expected", Type: "date", IsNullable: true, Position: 6},
				},
				Constraints: []sql_entities.SQLTableConstraint{},
				ForeignKeys: []sql_entities.SQLTableForeignKey{},
				Indexes:     []sql_entities.SQLTableIndex{},
			},
		},
	},
}

func TestPSQLReader(t *testing.T) {
	for image, data := range imageData {
		db, cleanup := setupPSQLContainer(t, image, data.DBName)

		dbReader := psql.NewPSQLDatabaseReader(db)
		testCheckDBIsEmpty(t, dbReader, data.IsEmpty)
		testListSchemaDependencies(t, dbReader, data.Sequences)
		testListSchemaNames(t, dbReader, data.Tables)
		testGetSchemaDefinition(t, dbReader, data.Tables)

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

func testListSchemaNames(t *testing.T, dbReader services.DatabaseReader, expectedData []sql_entities.SQLTable) {
	schemaNames, err := dbReader.ListSchemaNames()
	assert.Nil(t, err)
	assert.Equal(t, len(expectedData), len(schemaNames))

	for idx, table := range schemaNames {
		assert.Equal(t, expectedData[idx].Name, table)
	}
}

func testGetSchemaDefinition(t *testing.T, dbReader services.DatabaseReader, expectedData []sql_entities.SQLTable) {
	for _, expectedTable := range expectedData {
		schema, err := dbReader.GetSchemaDefinition(expectedTable.Name)
		assert.Nil(t, err)
		assert.NotNil(t, schema)

		table := schema.(*sql_entities.SQLTable)

		assert.Equal(t, expectedTable.Name, table.Name)
		assert.Equal(t, expectedTable.Columns, table.Columns)
		assert.Equal(t, expectedTable.Constraints, table.Constraints)
		assert.Equal(t, expectedTable.ForeignKeys, table.ForeignKeys)
		assert.Equal(t, expectedTable.Indexes, table.Indexes)
	}
}
