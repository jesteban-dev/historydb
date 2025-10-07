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

type PSQLTableContent struct {
	DataLength int
	MaxRowSize int
	ChunkSize  int64
	Rows       []sql_entities.SQLRecord
}

type PSQLTestContent struct {
	DBName       string
	IsEmpty      bool
	Sequences    []psql_entities.PSQLSequence
	Tables       []sql_entities.SQLTable
	TableContent map[string]PSQLTableContent
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
		TableContent: map[string]PSQLTableContent{
			"public.city": {DataLength: 4079, MaxRowSize: 84, ChunkSize: 200, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"id": int64(1), "name": "Kabul", "countrycode": "AFG", "district": "Kabol", "population": int64(1780000)}},
				{Content: map[string]interface{}{"id": int64(201), "name": "Sarajevo", "countrycode": "BIH", "district": "Federaatio", "population": int64(360000)}},
				{Content: map[string]interface{}{"id": int64(401), "name": "Ribeirão Pires", "countrycode": "BRA", "district": "São Paulo", "population": int64(108121)}},
				{Content: map[string]interface{}{"id": int64(601), "name": "Duran [Eloy Alfaro]", "countrycode": "ECU", "district": "Guayas", "population": int64(152514)}},
				{Content: map[string]interface{}{"id": int64(801), "name": "Cabanatuan", "countrycode": "PHL", "district": "Central Luzon", "population": int64(222859)}},
				{Content: map[string]interface{}{"id": int64(1001), "name": "Depok", "countrycode": "IDN", "district": "Yogyakarta", "population": int64(106800)}},
				{Content: map[string]interface{}{"id": int64(1201), "name": "Cuddalore", "countrycode": "IND", "district": "Tamil Nadu", "population": int64(153086)}},
				{Content: map[string]interface{}{"id": int64(1401), "name": "Khorramabad", "countrycode": "IRN", "district": "Lorestan", "population": int64(272815)}},
				{Content: map[string]interface{}{"id": int64(1601), "name": "Yokkaichi", "countrycode": "JPN", "district": "Mie", "population": int64(288173)}},
				{Content: map[string]interface{}{"id": int64(1801), "name": "Battambang", "countrycode": "KHM", "district": "Battambang", "population": int64(129800)}},
				{Content: map[string]interface{}{"id": int64(2001), "name": "Tengzhou", "countrycode": "CHN", "district": "Shandong", "population": int64(315083)}},
				{Content: map[string]interface{}{"id": int64(2201), "name": "Kaili", "countrycode": "CHN", "district": "Guizhou", "population": int64(113958)}},
				{Content: map[string]interface{}{"id": int64(2401), "name": "Athenai", "countrycode": "GRC", "district": "Attika", "population": int64(772072)}},
				{Content: map[string]interface{}{"id": int64(2601), "name": "Tecámac", "countrycode": "MEX", "district": "México", "population": int64(172410)}},
				{Content: map[string]interface{}{"id": int64(2801), "name": "Agege", "countrycode": "NGA", "district": "Lagos", "population": int64(105000)}},
				{Content: map[string]interface{}{"id": int64(3001), "name": "Metz", "countrycode": "FRA", "district": "Lorraine", "population": int64(123776)}},
				{Content: map[string]interface{}{"id": int64(3201), "name": "Ziguinchor", "countrycode": "SEN", "district": "Ziguinchor", "population": int64(192000)}},
				{Content: map[string]interface{}{"id": int64(3401), "name": "Kilis", "countrycode": "TUR", "district": "Kilis", "population": int64(118245)}},
				{Content: map[string]interface{}{"id": int64(3601), "name": "Habarovsk", "countrycode": "RUS", "district": "Habarovsk", "population": int64(609400)}},
				{Content: map[string]interface{}{"id": int64(3801), "name": "San Antonio", "countrycode": "USA", "district": "Texas", "population": int64(1144646)}},
				{Content: map[string]interface{}{"id": int64(4001), "name": "Gilbert", "countrycode": "USA", "district": "Arizona", "population": int64(109697)}},
			}},
			"public.country": {DataLength: 239, MaxRowSize: 206, ChunkSize: 20, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"code": "AFG", "name": "Afghanistan", "continent": "Asia", "region": "Southern and Central Asia", "surfacearea": float64(652090), "indepyear": int64(1919), "population": int64(22720000), "lifeexpectancy": float64(45.900002), "gnp": float64(5976), "gnpold": nil, "localname": "Afganistan/Afqanestan", "governmentform": "Islamic Emirate", "headofstate": "Mohammad Omar", "capital": int64(1), "code2": "AF"}},
				{Content: map[string]interface{}{"code": "BEL", "name": "Belgium", "continent": "Europe", "region": "Western Europe", "surfacearea": float64(30518), "indepyear": int64(1830), "population": int64(10239000), "lifeexpectancy": float64(77.800003), "gnp": float64(249704), "gnpold": float64(243948), "localname": "België/Belgique", "governmentform": "Constitutional Monarchy, Federation", "headofstate": "Albert II", "capital": int64(179), "code2": "BE"}},
				{Content: map[string]interface{}{"code": "DMA", "name": "Dominica", "continent": "North America", "region": "Caribbean", "surfacearea": float64(751), "indepyear": int64(1978), "population": int64(71000), "lifeexpectancy": float64(73.400002), "gnp": float64(256), "gnpold": float64(243), "localname": "Dominica", "governmentform": "Republic", "headofstate": "Vernon Shaw", "capital": int64(586), "code2": "DM"}},
				{Content: map[string]interface{}{"code": "GLP", "name": "Guadeloupe", "continent": "North America", "region": "Caribbean", "surfacearea": float64(1705), "indepyear": nil, "population": int64(456000), "lifeexpectancy": float64(77), "gnp": float64(3501), "gnpold": nil, "localname": "Guadeloupe", "governmentform": "Overseas Department of France", "headofstate": "Jacques Chirac", "capital": int64(919), "code2": "GP"}},
				{Content: map[string]interface{}{"code": "JAM", "name": "Jamaica", "continent": "North America", "region": "Caribbean", "surfacearea": float64(10990), "indepyear": int64(1962), "population": int64(2583000), "lifeexpectancy": float64(75.199997), "gnp": float64(6871), "gnpold": float64(6722), "localname": "Jamaica", "governmentform": "Constitutional Monarchy", "headofstate": "Elisabeth II", "capital": int64(1530), "code2": "JM"}},
				{Content: map[string]interface{}{"code": "CCK", "name": "Cocos (Keeling) Islands", "continent": "Oceania", "region": "Australia and New Zealand", "surfacearea": float64(14), "indepyear": nil, "population": int64(600), "lifeexpectancy": nil, "gnp": float64(0), "gnpold": nil, "localname": "Cocos (Keeling) Islands", "governmentform": "Territory of Australia", "headofstate": "Elisabeth II", "capital": int64(2317), "code2": "CC"}},
				{Content: map[string]interface{}{"code": "MKD", "name": "Macedonia", "continent": "Europe", "region": "Southern Europe", "surfacearea": float64(25713), "indepyear": int64(1991), "population": int64(2024000), "lifeexpectancy": float64(73.800003), "gnp": float64(1694), "gnpold": float64(1915), "localname": "Makedonija", "governmentform": "Republic", "headofstate": "Boris Trajkovski", "capital": int64(2460), "code2": "MK"}},
				{Content: map[string]interface{}{"code": "NAM", "name": "Namibia", "continent": "Africa", "region": "Southern Africa", "surfacearea": float64(824292), "indepyear": int64(1990), "population": int64(1726000), "lifeexpectancy": float64(42.5), "gnp": float64(3101), "gnpold": float64(3384), "localname": "Namibia", "governmentform": "Republic", "headofstate": "Sam Nujoma", "capital": int64(2726), "code2": "NA"}},
				{Content: map[string]interface{}{"code": "PRI", "name": "Puerto Rico", "continent": "North America", "region": "Caribbean", "surfacearea": float64(8875), "indepyear": nil, "population": int64(3869000), "lifeexpectancy": float64(75.599998), "gnp": float64(34100), "gnpold": float64(32100), "localname": "Puerto Rico", "governmentform": "Commonwealth of the US", "headofstate": "George W. Bush", "capital": int64(2919), "code2": "PR"}},
				{Content: map[string]interface{}{"code": "SMR", "name": "San Marino", "continent": "Europe", "region": "Southern Europe", "surfacearea": float64(61), "indepyear": int64(885), "population": int64(27000), "lifeexpectancy": float64(81.099998), "gnp": float64(510), "gnpold": nil, "localname": "San Marino", "governmentform": "Republic", "headofstate": nil, "capital": int64(3171), "code2": "SM"}},
				{Content: map[string]interface{}{"code": "DNK", "name": "Denmark", "continent": "Europe", "region": "Nordic Countries", "surfacearea": float64(43094), "indepyear": int64(800), "population": int64(5330000), "lifeexpectancy": float64(76.5), "gnp": float64(174099), "gnpold": float64(169264), "localname": "Danmark", "governmentform": "Constitutional Monarchy", "headofstate": "Margrethe II", "capital": int64(3315), "code2": "DK"}},
				{Content: map[string]interface{}{"code": "BLR", "name": "Belarus", "continent": "Europe", "region": "Eastern Europe", "surfacearea": float64(207600), "indepyear": int64(1991), "population": int64(10236000), "lifeexpectancy": float64(68), "gnp": float64(13714), "gnpold": nil, "localname": "Belarus", "governmentform": "Republic", "headofstate": "Aljaksandr Luka\u009aenka", "capital": int64(3520), "code2": "BY"}},
			}},
			"public.countrylanguage": {DataLength: 984, MaxRowSize: 60, ChunkSize: 50, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"countrycode": "AFG", "language": "Pashto", "isofficial": true, "percentage": 52.400002}},
				{Content: map[string]interface{}{"countrycode": "FJI", "language": "Fijian", "isofficial": true, "percentage": 50.799999}},
				{Content: map[string]interface{}{"countrycode": "CCK", "language": "Malay", "isofficial": false, "percentage": float64(0)}},
				{Content: map[string]interface{}{"countrycode": "OMN", "language": "Arabic", "isofficial": true, "percentage": 76.699997}},
				{Content: map[string]interface{}{"countrycode": "DNK", "language": "Danish", "isofficial": true, "percentage": 93.5}},
				{Content: map[string]interface{}{"countrycode": "BGD", "language": "Chakma", "isofficial": false, "percentage": 0.40000001}},
				{Content: map[string]interface{}{"countrycode": "IRL", "language": "Irish", "isofficial": true, "percentage": 1.6}},
				{Content: map[string]interface{}{"countrycode": "MAR", "language": "Berberi", "isofficial": false, "percentage": float64(33)}},
				{Content: map[string]interface{}{"countrycode": "SYC", "language": "English", "isofficial": true, "percentage": 3.8}},
				{Content: map[string]interface{}{"countrycode": "AND", "language": "Portuguese", "isofficial": false, "percentage": 10.8}},
				{Content: map[string]interface{}{"countrycode": "CAN", "language": "Chinese", "isofficial": false, "percentage": 2.5}},
				{Content: map[string]interface{}{"countrycode": "REU", "language": "Comorian", "isofficial": false, "percentage": 2.8}},
				{Content: map[string]interface{}{"countrycode": "AZE", "language": "Armenian", "isofficial": false, "percentage": float64(2)}},
				{Content: map[string]interface{}{"countrycode": "LBR", "language": "Gio", "isofficial": false, "percentage": 7.9000001}},
				{Content: map[string]interface{}{"countrycode": "TKM", "language": "Kazakh", "isofficial": false, "percentage": float64(2)}},
				{Content: map[string]interface{}{"countrycode": "COD", "language": "Zande", "isofficial": false, "percentage": 6.0999999}},
				{Content: map[string]interface{}{"countrycode": "EST", "language": "Finnish", "isofficial": false, "percentage": 0.69999999}},
				{Content: map[string]interface{}{"countrycode": "ROM", "language": "Serbo-Croatian", "isofficial": false, "percentage": 0.1}},
				{Content: map[string]interface{}{"countrycode": "DNK", "language": "Norwegian", "isofficial": false, "percentage": 0.30000001}},
				{Content: map[string]interface{}{"countrycode": "NGA", "language": "Ijo", "isofficial": false, "percentage": 1.8}},
			}},
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
		TableContent: map[string]PSQLTableContent{
			"public.categories": {DataLength: 16, MaxRowSize: 40, ChunkSize: 4, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"category": int64(1), "categoryname": "Action"}},
				{Content: map[string]interface{}{"category": int64(5), "categoryname": "Comedy"}},
				{Content: map[string]interface{}{"category": int64(9), "categoryname": "Foreign"}},
				{Content: map[string]interface{}{"category": int64(13), "categoryname": "New"}},
			}},
			"public.customers": {DataLength: 20000, MaxRowSize: 194, ChunkSize: 2000, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"customerid": int64(1), "firstname": "VKUUXF", "lastname": "ITHOMQJNYX", "address1": "4608499546 Dell Way", "address2": nil, "city": "QSDPAGD", "state": "SD", "zip": int64(24101), "country": "US", "region": int64(1), "email": "ITHOMQJNYX@dell.com", "phone": "4608499546", "creditcardtype": int64(1), "creditcard": "1979279217775911", "creditcardexpiration": "2012/03", "username": "user1", "password": "password", "age": int64(55), "income": int64(100000), "gender": "M"}},
				{Content: map[string]interface{}{"customerid": int64(2001), "firstname": "LTMAHF", "lastname": "LNNBYHHIGY", "address1": "1001614924 Dell Way", "address2": nil, "city": "XTLGXRX", "state": "MD", "zip": int64(60295), "country": "US", "region": int64(1), "email": "LNNBYHHIGY@dell.com", "phone": "1001614924", "creditcardtype": int64(2), "creditcard": "4158183055929840", "creditcardexpiration": "2011/09", "username": "user2001", "password": "password", "age": int64(74), "income": int64(20000), "gender": "F"}},
				{Content: map[string]interface{}{"customerid": int64(4001), "firstname": "IMYAYU", "lastname": "YTJXGCFHWJ", "address1": "4942974925 Dell Way", "address2": nil, "city": "EVYIIYY", "state": "LA", "zip": int64(87912), "country": "US", "region": int64(1), "email": "YTJXGCFHWJ@dell.com", "phone": "4942974925", "creditcardtype": int64(2), "creditcard": "3971776042133570", "creditcardexpiration": "2010/01", "username": "user4001", "password": "password", "age": int64(22), "income": int64(100000), "gender": "M"}},
				{Content: map[string]interface{}{"customerid": int64(6001), "firstname": "RZKRNQ", "lastname": "ZRDXBZHLZM", "address1": "4214268075 Dell Way", "address2": nil, "city": "KBTHSEF", "state": "ND", "zip": int64(23997), "country": "US", "region": int64(1), "email": "ZRDXBZHLZM@dell.com", "phone": "4214268075", "creditcardtype": int64(2), "creditcard": "7078812733292580", "creditcardexpiration": "2010/11", "username": "user6001", "password": "password", "age": int64(70), "income": int64(40000), "gender": "M"}},
				{Content: map[string]interface{}{"customerid": int64(8001), "firstname": "RTYKLZ", "lastname": "AWHGWKNKMF", "address1": "9094764851 Dell Way", "address2": nil, "city": "LAZWJQW", "state": "WY", "zip": int64(34834), "country": "US", "region": int64(1), "email": "AWHGWKNKMF@dell.com", "phone": "9094764851", "creditcardtype": int64(5), "creditcard": "5071015951689333", "creditcardexpiration": "2008/04", "username": "user8001", "password": "password", "age": int64(38), "income": int64(80000), "gender": "F"}},
				{Content: map[string]interface{}{"customerid": int64(10001), "firstname": "QQGWSL", "lastname": "NNTHRFAVRX", "address1": "6739733410 Dell Way", "address2": nil, "city": "KQLKYDL", "state": nil, "zip": int64(0), "country": "UK", "region": int64(2), "email": "NNTHRFAVRX@dell.com", "phone": "6739733410", "creditcardtype": int64(5), "creditcard": "3180290846154094", "creditcardexpiration": "2010/06", "username": "user10001", "password": "password", "age": int64(62), "income": int64(100000), "gender": "F"}},
				{Content: map[string]interface{}{"customerid": int64(12001), "firstname": "KKPQBY", "lastname": "JDLUPSWWSD", "address1": "7087720097 Dell Way", "address2": nil, "city": "FMJQJVB", "state": nil, "zip": int64(0), "country": "Chile", "region": int64(2), "email": "JDLUPSWWSD@dell.com", "phone": "7087720097", "creditcardtype": int64(5), "creditcard": "6277906919363887", "creditcardexpiration": "2011/11", "username": "user12001", "password": "password", "age": int64(63), "income": int64(20000), "gender": "F"}},
				{Content: map[string]interface{}{"customerid": int64(14001), "firstname": "BPGIXX", "lastname": "XWRAGPSDPR", "address1": "6341127964 Dell Way", "address2": nil, "city": "HCVCEGB", "state": nil, "zip": int64(0), "country": "Germany", "region": int64(2), "email": "XWRAGPSDPR@dell.com", "phone": "6341127964", "creditcardtype": int64(5), "creditcard": "2779878668952733", "creditcardexpiration": "2012/05", "username": "user14001", "password": "password", "age": int64(41), "income": int64(80000), "gender": "F"}},
				{Content: map[string]interface{}{"customerid": int64(16001), "firstname": "NLNNKJ", "lastname": "OFPQTZQNZG", "address1": "9248454082 Dell Way", "address2": nil, "city": "UXEKOQN", "state": nil, "zip": int64(0), "country": "Australia", "region": int64(2), "email": "OFPQTZQNZG@dell.com", "phone": "9248454082", "creditcardtype": int64(3), "creditcard": "3312776351813227", "creditcardexpiration": "2009/08", "username": "user16001", "password": "password", "age": int64(75), "income": int64(40000), "gender": "F"}},
				{Content: map[string]interface{}{"customerid": int64(18001), "firstname": "XWYOVV", "lastname": "OFBFKFIRLM", "address1": "2933766882 Dell Way", "address2": nil, "city": "ONXARPP", "state": nil, "zip": int64(0), "country": "Canada", "region": int64(2), "email": "OFBFKFIRLM@dell.com", "phone": "2933766882", "creditcardtype": int64(3), "creditcard": "1694544910918921", "creditcardexpiration": "2011/07", "username": "user18001", "password": "password", "age": int64(67), "income": int64(20000), "gender": "F"}},
			}},
			"public.cust_hist": {DataLength: 60350, MaxRowSize: 36, ChunkSize: 10000, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"customerid": int64(7888), "orderid": int64(1), "prod_id": int64(9117)}},
				{Content: map[string]interface{}{"customerid": int64(12033), "orderid": int64(1985), "prod_id": int64(3844)}},
				{Content: map[string]interface{}{"customerid": int64(13953), "orderid": int64(3958), "prod_id": int64(7382)}},
				{Content: map[string]interface{}{"customerid": int64(7638), "orderid": int64(5946), "prod_id": int64(6664)}},
				{Content: map[string]interface{}{"customerid": int64(9534), "orderid": int64(7947), "prod_id": int64(7549)}},
				{Content: map[string]interface{}{"customerid": int64(760), "orderid": int64(9925), "prod_id": int64(1585)}},
				{Content: map[string]interface{}{"customerid": int64(11438), "orderid": int64(11927), "prod_id": int64(948)}},
			}},
			"public.inventory": {DataLength: 10000, MaxRowSize: 36, ChunkSize: 2000, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"prod_id": int64(1), "quan_in_stock": int64(138), "sales": int64(9)}},
				{Content: map[string]interface{}{"prod_id": int64(2001), "quan_in_stock": int64(235), "sales": int64(6)}},
				{Content: map[string]interface{}{"prod_id": int64(4001), "quan_in_stock": int64(163), "sales": int64(15)}},
				{Content: map[string]interface{}{"prod_id": int64(6001), "quan_in_stock": int64(218), "sales": int64(17)}},
				{Content: map[string]interface{}{"prod_id": int64(8001), "quan_in_stock": int64(152), "sales": int64(2)}},
			}},
			"public.orderlines": {DataLength: 60350, MaxRowSize: 44, ChunkSize: 10000, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"orderlineid": int64(1), "orderid": int64(1), "prod_id": int64(9117), "quantity": int64(1), "orderdate": time.Date(2004, 1, 27, 0, 0, 0, 0, time.UTC)}},
				{Content: map[string]interface{}{"orderlineid": int64(2), "orderid": int64(1985), "prod_id": int64(3844), "quantity": int64(3), "orderdate": time.Date(2004, 2, 28, 0, 0, 0, 0, time.UTC)}},
				{Content: map[string]interface{}{"orderlineid": int64(4), "orderid": int64(3958), "prod_id": int64(7382), "quantity": int64(3), "orderdate": time.Date(2004, 4, 15, 0, 0, 0, 0, time.UTC)}},
				{Content: map[string]interface{}{"orderlineid": int64(6), "orderid": int64(5946), "prod_id": int64(6664), "quantity": int64(2), "orderdate": time.Date(2004, 6, 18, 0, 0, 0, 0, time.UTC)}},
				{Content: map[string]interface{}{"orderlineid": int64(2), "orderid": int64(7947), "prod_id": int64(7549), "quantity": int64(3), "orderdate": time.Date(2004, 8, 6, 0, 0, 0, 0, time.UTC)}},
				{Content: map[string]interface{}{"orderlineid": int64(1), "orderid": int64(9925), "prod_id": int64(1585), "quantity": int64(1), "orderdate": time.Date(2004, 10, 31, 0, 0, 0, 0, time.UTC)}},
				{Content: map[string]interface{}{"orderlineid": int64(7), "orderid": int64(11927), "prod_id": int64(948), "quantity": int64(3), "orderdate": time.Date(2004, 12, 25, 0, 0, 0, 0, time.UTC)}},
			}},
			"public.orders": {DataLength: 12000, MaxRowSize: 57, ChunkSize: 4000, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"orderid": int64(1), "orderdate": time.Date(2004, 1, 27, 0, 0, 0, 0, time.UTC), "customerid": int64(7888), "netamount": float64(313.24), "tax": float64(25.84), "totalamount": float64(339.08)}},
				{Content: map[string]interface{}{"orderid": int64(4001), "orderdate": time.Date(2004, 5, 12, 0, 0, 0, 0, time.UTC), "customerid": int64(14005), "netamount": float64(350.6), "tax": float64(28.92), "totalamount": float64(379.52)}},
				{Content: map[string]interface{}{"orderid": int64(8001), "orderdate": time.Date(2004, 9, 12, 0, 0, 0, 0, time.UTC), "customerid": int64(9922), "netamount": float64(384.52), "tax": float64(31.72), "totalamount": float64(416.24)}},
			}},
			"public.products": {DataLength: 10000, MaxRowSize: 92, ChunkSize: 2000, Rows: []sql_entities.SQLRecord{
				{Content: map[string]interface{}{"prod_id": int64(1), "category": int64(14), "title": "ACADEMY ACADEMY", "actor": "PENELOPE GUINESS", "price": float64(25.99), "special": int64(0), "common_prod_id": int64(1976)}},
				{Content: map[string]interface{}{"prod_id": int64(2001), "category": int64(5), "title": "ADAPTATION ACADEMY", "actor": "CARY NEESON", "price": float64(19.99), "special": int64(0), "common_prod_id": int64(3385)}},
				{Content: map[string]interface{}{"prod_id": int64(4001), "category": int64(11), "title": "AFRICAN ACADEMY", "actor": "INGRID DOUGLAS", "price": float64(20.99), "special": int64(0), "common_prod_id": int64(9099)}},
				{Content: map[string]interface{}{"prod_id": int64(6001), "category": int64(1), "title": "AIRPLANE ACADEMY", "actor": "TIM BRIDGES", "price": float64(22.99), "special": int64(0), "common_prod_id": int64(5905)}},
				{Content: map[string]interface{}{"prod_id": int64(8001), "category": int64(13), "title": "ALABAMA ACADEMY", "actor": "ANNETTE KINNEAR", "price": float64(11.99), "special": int64(0), "common_prod_id": int64(2439)}},
			}},
			"public.reorder": {ChunkSize: 1},
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
		testGetSchemaRecordMetadata(t, dbReader, data.TableContent)
		testGetSchemaRecordChunk(t, dbReader, data.TableContent)

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

func testGetSchemaRecordMetadata(t *testing.T, dbReader services.DatabaseReader, expectedData map[string]PSQLTableContent) {
	for tableName, test := range expectedData {
		metadata, err := dbReader.GetSchemaRecordMetadata(tableName)
		assert.Nil(t, err)
		assert.Equal(t, test.DataLength, metadata.Count)
		assert.Equal(t, test.MaxRowSize, metadata.MaxRecordSize)
	}
}

func testGetSchemaRecordChunk(t *testing.T, dbReader services.DatabaseReader, expectedData map[string]PSQLTableContent) {
	for tableName, test := range expectedData {
		schema, err := dbReader.GetSchemaDefinition(tableName)
		assert.Nil(t, err)
		assert.NotNil(t, schema)

		var cursor interface{} = nil
		expectedRecordIndex := 0
		for {
			chunk, nextCursor, err := dbReader.GetSchemaRecordChunk(schema, test.ChunkSize, cursor)
			assert.Nil(t, err)

			chunkRecord := chunk.(*sql_entities.SQLRecordChunk)
			if len(chunkRecord.Content) == 0 {
				break
			} else {
				assert.LessOrEqual(t, len(chunkRecord.Content), int(test.ChunkSize))

				for k, v := range test.Rows[expectedRecordIndex].Content {
					switch a := v.(type) {
					case time.Time:
						var recordValue interface{}
						for key, value := range chunkRecord.Content[0].Content {
							if key == k {
								recordValue = value
							}
						}
						e, ok := recordValue.(time.Time)
						assert.True(t, ok)
						assert.Equal(t, e.Unix(), a.Unix())
					default:
						var recordValue interface{}
						for key, value := range chunkRecord.Content[0].Content {
							if key == k {
								recordValue = value
							}
						}
						assert.Equal(t, v, recordValue)
					}
				}
				expectedRecordIndex++
			}

			cursor = nextCursor
		}
	}
}
