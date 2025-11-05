package test

import (
	"context"
	"database/sql"
	"fmt"
	"historydb/src/internal/helpers"
	"historydb/src/internal/services"
	"historydb/src/internal/services/database_impl"
	"historydb/src/internal/services/entities"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var imageData = map[string]DBSQLTestContent{
	"aa8y/postgres-dataset:world": {
		DBName: "world",
		Tables: []entities.SQLTable{
			{
				TableName: "public.city",
				Columns: []entities.SQLTableColumn{
					{Name: "id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "name", Type: "text", IsNullable: false, Position: 2},
					{Name: "countrycode", Type: "character(3)", IsNullable: false, Position: 3},
					{Name: "district", Type: "text", IsNullable: false, Position: 4},
					{Name: "population", Type: "integer", IsNullable: false, Position: 5},
				},
				Constraints: []entities.SQLTableConstraint{
					{Type: entities.PrimaryKey, Name: "city_pkey", Columns: []string{"id"}},
				},
			}, {
				TableName: "public.country",
				Columns: []entities.SQLTableColumn{
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
				Constraints: []entities.SQLTableConstraint{
					{Type: entities.Check, Name: "country_continent_check", Definition: helpers.Pointer("((continent = 'Asia'::text) OR (continent = 'Europe'::text) OR (continent = 'North America'::text) OR (continent = 'Africa'::text) OR (continent = 'Oceania'::text) OR (continent = 'Antarctica'::text) OR (continent = 'South America'::text))")},
					{Type: entities.PrimaryKey, Name: "country_pkey", Columns: []string{"code"}},
				},
				ForeignKeys: []entities.SQLTableForeignKey{
					{Name: "country_capital_fkey", Columns: []string{"capital"}, ReferencedTable: "public.city", ReferencedColumns: []string{"id"}, UpdateAction: entities.NoAction, DeleteAction: entities.NoAction},
				},
			}, {
				TableName: "public.countrylanguage",
				Columns: []entities.SQLTableColumn{
					{Name: "countrycode", Type: "character(3)", IsNullable: false, Position: 1},
					{Name: "language", Type: "text", IsNullable: false, Position: 2},
					{Name: "isofficial", Type: "boolean", IsNullable: false, Position: 3},
					{Name: "percentage", Type: "real", IsNullable: false, Position: 4},
				},
				Constraints: []entities.SQLTableConstraint{
					{Type: entities.PrimaryKey, Name: "countrylanguage_pkey", Columns: []string{"countrycode", "language"}},
				},
				ForeignKeys: []entities.SQLTableForeignKey{
					{Name: "countrylanguage_countrycode_fkey", Columns: []string{"countrycode"}, ReferencedTable: "public.country", ReferencedColumns: []string{"code"}, UpdateAction: entities.NoAction, DeleteAction: entities.NoAction},
				},
			},
		},
	},
	"aa8y/postgres-dataset:dellstore": {
		DBName: "dellstore",
		Tables: []entities.SQLTable{
			{
				TableName: "public.categories",
				Columns: []entities.SQLTableColumn{
					{Name: "category", Type: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('categories_category_seq'::regclass)"), Position: 1},
					{Name: "categoryname", Type: "character varying(50)", IsNullable: false, Position: 2},
				},
				Constraints: []entities.SQLTableConstraint{
					{Type: entities.PrimaryKey, Name: "categories_pkey", Columns: []string{"category"}},
				},
			}, {
				TableName: "public.cust_hist",
				Columns: []entities.SQLTableColumn{
					{Name: "customerid", Type: "integer", IsNullable: false, Position: 1},
					{Name: "orderid", Type: "integer", IsNullable: false, Position: 2},
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 3},
				},
				ForeignKeys: []entities.SQLTableForeignKey{
					{Name: "fk_cust_hist_customerid", Columns: []string{"customerid"}, ReferencedTable: "public.customers", ReferencedColumns: []string{"customerid"}, UpdateAction: entities.NoAction, DeleteAction: entities.Cascade},
				},
				Indexes: []entities.SQLTableIndex{
					{Name: "ix_cust_hist_customerid", Type: "btree", Columns: []string{"customerid"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.customers",
				Columns: []entities.SQLTableColumn{
					{Name: "customerid", Type: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('customers_customerid_seq'::regclass)"), Position: 1},
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
				Constraints: []entities.SQLTableConstraint{
					{Type: entities.PrimaryKey, Name: "customers_pkey", Columns: []string{"customerid"}},
				},
				Indexes: []entities.SQLTableIndex{
					{Name: "ix_cust_username", Type: "btree", Columns: []string{"username"}, Options: map[string]interface{}{"isUnique": true}},
				},
			}, {
				TableName: "public.inventory",
				Columns: []entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "quan_in_stock", Type: "integer", IsNullable: false, Position: 2},
					{Name: "sales", Type: "integer", IsNullable: false, Position: 3},
				},
				Constraints: []entities.SQLTableConstraint{
					{Type: entities.PrimaryKey, Name: "inventory_pkey", Columns: []string{"prod_id"}},
				},
			}, {
				TableName: "public.orderlines",
				Columns: []entities.SQLTableColumn{
					{Name: "orderlineid", Type: "integer", IsNullable: false, Position: 1},
					{Name: "orderid", Type: "integer", IsNullable: false, Position: 2},
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 3},
					{Name: "quantity", Type: "smallint", IsNullable: false, Position: 4},
					{Name: "orderdate", Type: "date", IsNullable: false, Position: 5},
				},
				ForeignKeys: []entities.SQLTableForeignKey{
					{Name: "fk_orderid", Columns: []string{"orderid"}, ReferencedTable: "public.orders", ReferencedColumns: []string{"orderid"}, UpdateAction: entities.NoAction, DeleteAction: entities.Cascade},
				},
				Indexes: []entities.SQLTableIndex{
					{Name: "ix_orderlines_orderid", Type: "btree", Columns: []string{"orderid", "orderlineid"}, Options: map[string]interface{}{"isUnique": true}},
				},
			}, {
				TableName: "public.orders",
				Columns: []entities.SQLTableColumn{
					{Name: "orderid", Type: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('orders_orderid_seq'::regclass)"), Position: 1},
					{Name: "orderdate", Type: "date", IsNullable: false, Position: 2},
					{Name: "customerid", Type: "integer", IsNullable: true, Position: 3},
					{Name: "netamount", Type: "numeric(12,2)", IsNullable: false, Position: 4},
					{Name: "tax", Type: "numeric(12,2)", IsNullable: false, Position: 5},
					{Name: "totalamount", Type: "numeric(12,2)", IsNullable: false, Position: 6},
				},
				Constraints: []entities.SQLTableConstraint{
					{Type: entities.PrimaryKey, Name: "orders_pkey", Columns: []string{"orderid"}},
				},
				ForeignKeys: []entities.SQLTableForeignKey{
					{Name: "fk_customerid", Columns: []string{"customerid"}, ReferencedTable: "public.customers", ReferencedColumns: []string{"customerid"}, UpdateAction: entities.NoAction, DeleteAction: entities.SetNull},
				},
				Indexes: []entities.SQLTableIndex{
					{Name: "ix_order_custid", Type: "btree", Columns: []string{"customerid"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.products",
				Columns: []entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('products_prod_id_seq'::regclass)"), Position: 1},
					{Name: "category", Type: "integer", IsNullable: false, Position: 2},
					{Name: "title", Type: "character varying(50)", IsNullable: false, Position: 3},
					{Name: "actor", Type: "character varying(50)", IsNullable: false, Position: 4},
					{Name: "price", Type: "numeric(12,2)", IsNullable: false, Position: 5},
					{Name: "special", Type: "smallint", IsNullable: true, Position: 6},
					{Name: "common_prod_id", Type: "integer", IsNullable: false, Position: 7},
				},
				Constraints: []entities.SQLTableConstraint{
					{Type: entities.PrimaryKey, Name: "products_pkey", Columns: []string{"prod_id"}},
				},
				Indexes: []entities.SQLTableIndex{
					{Name: "ix_prod_category", Type: "btree", Columns: []string{"category"}, Options: map[string]interface{}{"isUnique": false}},
					{Name: "ix_prod_special", Type: "btree", Columns: []string{"special"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.reorder",
				Columns: []entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "date_low", Type: "date", IsNullable: false, Position: 2},
					{Name: "quan_low", Type: "integer", IsNullable: false, Position: 3},
					{Name: "date_reordered", Type: "date", IsNullable: true, Position: 4},
					{Name: "quan_reordered", Type: "integer", IsNullable: true, Position: 5},
					{Name: "date_expected", Type: "date", IsNullable: true, Position: 6},
				},
			},
		},
	},
}

func runPSQLReaderTests(t *testing.T) {
	for image, data := range imageData {
		db, cleanup := setupPostgreSQLContainer(t, image, data.DBName)

		dbReader := database_impl.NewPSQLDatabaseReader(db)
		testListSchemaNames(t, dbReader, data)
		testGetSchemaDefinition(t, dbReader, data)

		cleanup()
	}
}

func setupPostgreSQLContainer(t *testing.T, image string, dbName string) (*sql.DB, func()) {
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
		t.Fatalf("impossible to generate container for: %s | %v", image, err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal("impossible to get container host: ", err)
	}

	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal("impossible to get container mapped port: ", err)
	}

	dbUrl := fmt.Sprintf("postgres://test:test@%s:%s/%s?sslmode=disable", host, port.Port(), dbName)
	dsn, err := parseDbUrl(dbUrl)
	if err != nil {
		t.Fatal("failed to parse db url: ", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("failed to connect to postgres database: ", err)
	}

	cleanup := func() {
		container.Terminate(ctx)
	}

	return db, cleanup
}

func testListSchemaNames(t *testing.T, dbReader services.DatabaseReader, expectedData DBSQLTestContent) {
	schemaNames, err := dbReader.ListSchemaNames()
	assert.Nil(t, err)
	assert.NotNil(t, schemaNames)
	assert.Equal(t, len(expectedData.Tables), len(schemaNames))

	expectedNames := make([]string, 0, len(expectedData.Tables))
	for _, table := range expectedData.Tables {
		expectedNames = append(expectedNames, table.TableName)
	}

	assert.Equal(t, expectedNames, schemaNames)
}

func testGetSchemaDefinition(t *testing.T, dbReader services.DatabaseReader, expectedData DBSQLTestContent) {
	for _, expectedTable := range expectedData.Tables {
		schema, err := dbReader.GetSchemaDefinition(expectedTable.TableName)
		assert.Nil(t, err)
		assert.NotNil(t, schema)

		table := schema.(*entities.SQLTable)

		assert.Equal(t, expectedTable.TableName, table.TableName)
		assert.Equal(t, len(expectedTable.Columns), len(table.Columns))
		assert.Equal(t, len(expectedTable.Constraints), len(table.Constraints))
		assert.Equal(t, len(expectedTable.ForeignKeys), len(table.ForeignKeys))
		assert.Equal(t, len(expectedTable.Indexes), len(table.Indexes))

		for j, column := range table.Columns {
			assert.Equal(t, expectedTable.Columns[j].Name, column.Name)
			assert.Equal(t, expectedTable.Columns[j].Type, column.Type)
			assert.Equal(t, expectedTable.Columns[j].IsNullable, column.IsNullable)
			assert.Equal(t, expectedTable.Columns[j].DefaultValue, column.DefaultValue)
			assert.Equal(t, expectedTable.Columns[j].Position, column.Position)
		}

		for j, constraint := range table.Constraints {
			assert.Equal(t, expectedTable.Constraints[j].Type, constraint.Type)
			assert.Equal(t, expectedTable.Constraints[j].Name, constraint.Name)
			assert.Equal(t, expectedTable.Constraints[j].Columns, constraint.Columns)
			assert.Equal(t, expectedTable.Constraints[j].Definition, constraint.Definition)
		}

		for j, fKey := range table.ForeignKeys {
			assert.Equal(t, expectedTable.ForeignKeys[j].Name, fKey.Name)
			assert.Equal(t, expectedTable.ForeignKeys[j].Columns, fKey.Columns)
			assert.Equal(t, expectedTable.ForeignKeys[j].ReferencedTable, fKey.ReferencedTable)
			assert.Equal(t, expectedTable.ForeignKeys[j].ReferencedColumns, fKey.ReferencedColumns)
			assert.Equal(t, expectedTable.ForeignKeys[j].UpdateAction, fKey.UpdateAction)
			assert.Equal(t, expectedTable.ForeignKeys[j].DeleteAction, fKey.DeleteAction)
		}

		for j, index := range table.Indexes {
			assert.Equal(t, expectedTable.Indexes[j].Name, index.Name)
			assert.Equal(t, expectedTable.Indexes[j].Type, index.Type)
			assert.Equal(t, expectedTable.Indexes[j].Columns, index.Columns)
			assert.Equal(t, expectedTable.Indexes[j].Options, index.Options)
		}
	}
}
