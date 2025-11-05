package test

import (
	"context"
	"database/sql"
	"fmt"
	"historydb/src/internal/helpers"
	"historydb/src/internal/services"
	"historydb/src/internal/services/database_impl"
	"historydb/src/internal/services/database_impl/entities"
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
				Columns: []entities.TableColumn{
					{ColumnName: "id", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(1)},
					{ColumnName: "name", ColumnType: "text", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "countrycode", ColumnType: "character(3)", IsNullable: false, ColumnPosition: uint(3)},
					{ColumnName: "district", ColumnType: "text", IsNullable: false, ColumnPosition: uint(4)},
					{ColumnName: "population", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(5)},
				},
				Constraints: []entities.TableConstraint{
					{ConstraintType: entities.PrimaryKey, ConstraintName: "city_pkey", Columns: []string{"id"}},
				},
			}, {
				TableName: "public.country",
				Columns: []entities.TableColumn{
					{ColumnName: "code", ColumnType: "character(3)", IsNullable: false, ColumnPosition: uint(1)},
					{ColumnName: "name", ColumnType: "text", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "continent", ColumnType: "text", IsNullable: false, ColumnPosition: uint(3)},
					{ColumnName: "region", ColumnType: "text", IsNullable: false, ColumnPosition: uint(4)},
					{ColumnName: "surfacearea", ColumnType: "real", IsNullable: false, ColumnPosition: uint(5)},
					{ColumnName: "indepyear", ColumnType: "smallint", IsNullable: true, ColumnPosition: uint(6)},
					{ColumnName: "population", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(7)},
					{ColumnName: "lifeexpectancy", ColumnType: "real", IsNullable: true, ColumnPosition: uint(8)},
					{ColumnName: "gnp", ColumnType: "numeric(10,2)", IsNullable: true, ColumnPosition: uint(9)},
					{ColumnName: "gnpold", ColumnType: "numeric(10,2)", IsNullable: true, ColumnPosition: uint(10)},
					{ColumnName: "localname", ColumnType: "text", IsNullable: false, ColumnPosition: uint(11)},
					{ColumnName: "governmentform", ColumnType: "text", IsNullable: false, ColumnPosition: uint(12)},
					{ColumnName: "headofstate", ColumnType: "text", IsNullable: true, ColumnPosition: uint(13)},
					{ColumnName: "capital", ColumnType: "integer", IsNullable: true, ColumnPosition: uint(14)},
					{ColumnName: "code2", ColumnType: "character(2)", IsNullable: false, ColumnPosition: uint(15)},
				},
				Constraints: []entities.TableConstraint{
					{ConstraintType: entities.Check, ConstraintName: "country_continent_check", Definition: helpers.Pointer("((continent = 'Asia'::text) OR (continent = 'Europe'::text) OR (continent = 'North America'::text) OR (continent = 'Africa'::text) OR (continent = 'Oceania'::text) OR (continent = 'Antarctica'::text) OR (continent = 'South America'::text))")},
					{ConstraintType: entities.PrimaryKey, ConstraintName: "country_pkey", Columns: []string{"code"}},
				},
				ForeignKeys: []entities.ForeignKey{
					{ConstraintName: "country_capital_fkey", Columns: []string{"capital"}, ReferencedTable: "public.city", ReferencedColumns: []string{"id"}, UpdateAction: entities.NoAction, DeleteAction: entities.NoAction},
				},
			}, {
				TableName: "public.countrylanguage",
				Columns: []entities.TableColumn{
					{ColumnName: "countrycode", ColumnType: "character(3)", IsNullable: false, ColumnPosition: uint(1)},
					{ColumnName: "language", ColumnType: "text", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "isofficial", ColumnType: "boolean", IsNullable: false, ColumnPosition: uint(3)},
					{ColumnName: "percentage", ColumnType: "real", IsNullable: false, ColumnPosition: uint(4)},
				},
				Constraints: []entities.TableConstraint{
					{ConstraintType: entities.PrimaryKey, ConstraintName: "countrylanguage_pkey", Columns: []string{"countrycode", "language"}},
				},
				ForeignKeys: []entities.ForeignKey{
					{ConstraintName: "countrylanguage_countrycode_fkey", Columns: []string{"countrycode"}, ReferencedTable: "public.country", ReferencedColumns: []string{"code"}, UpdateAction: entities.NoAction, DeleteAction: entities.NoAction},
				},
			},
		},
		TableContent: map[string]DBSQLTableContent{
			"public.city": {BatchSize: 200, Rows: []entities.TableRow{
				{"id": int64(1), "name": "Kabul", "countrycode": "AFG", "district": "Kabol", "population": int64(1780000)},
				{"id": int64(201), "name": "Sarajevo", "countrycode": "BIH", "district": "Federaatio", "population": int64(360000)},
				{"id": int64(401), "name": "Ribeirão Pires", "countrycode": "BRA", "district": "São Paulo", "population": int64(108121)},
				{"id": int64(601), "name": "Duran [Eloy Alfaro]", "countrycode": "ECU", "district": "Guayas", "population": int64(152514)},
				{"id": int64(801), "name": "Cabanatuan", "countrycode": "PHL", "district": "Central Luzon", "population": int64(222859)},
				{"id": int64(1001), "name": "Depok", "countrycode": "IDN", "district": "Yogyakarta", "population": int64(106800)},
				{"id": int64(1201), "name": "Cuddalore", "countrycode": "IND", "district": "Tamil Nadu", "population": int64(153086)},
				{"id": int64(1401), "name": "Khorramabad", "countrycode": "IRN", "district": "Lorestan", "population": int64(272815)},
				{"id": int64(1601), "name": "Yokkaichi", "countrycode": "JPN", "district": "Mie", "population": int64(288173)},
				{"id": int64(1801), "name": "Battambang", "countrycode": "KHM", "district": "Battambang", "population": int64(129800)},
				{"id": int64(2001), "name": "Tengzhou", "countrycode": "CHN", "district": "Shandong", "population": int64(315083)},
				{"id": int64(2201), "name": "Kaili", "countrycode": "CHN", "district": "Guizhou", "population": int64(113958)},
				{"id": int64(2401), "name": "Athenai", "countrycode": "GRC", "district": "Attika", "population": int64(772072)},
				{"id": int64(2601), "name": "Tecámac", "countrycode": "MEX", "district": "México", "population": int64(172410)},
				{"id": int64(2801), "name": "Agege", "countrycode": "NGA", "district": "Lagos", "population": int64(105000)},
				{"id": int64(3001), "name": "Metz", "countrycode": "FRA", "district": "Lorraine", "population": int64(123776)},
				{"id": int64(3201), "name": "Ziguinchor", "countrycode": "SEN", "district": "Ziguinchor", "population": int64(192000)},
				{"id": int64(3401), "name": "Kilis", "countrycode": "TUR", "district": "Kilis", "population": int64(118245)},
				{"id": int64(3601), "name": "Habarovsk", "countrycode": "RUS", "district": "Habarovsk", "population": int64(609400)},
				{"id": int64(3801), "name": "San Antonio", "countrycode": "USA", "district": "Texas", "population": int64(1144646)},
				{"id": int64(4001), "name": "Gilbert", "countrycode": "USA", "district": "Arizona", "population": int64(109697)},
			}},
			"public.country": {BatchSize: 20, Rows: []entities.TableRow{
				{"code": "AFG", "name": "Afghanistan", "continent": "Asia", "region": "Southern and Central Asia", "surfacearea": float64(652090), "indepyear": int64(1919), "population": int64(22720000), "lifeexpectancy": float64(45.900002), "gnp": float64(5976), "gnpold": nil, "localname": "Afganistan/Afqanestan", "governmentform": "Islamic Emirate", "headofstate": "Mohammad Omar", "capital": int64(1), "code2": "AF"},
				{"code": "BEL", "name": "Belgium", "continent": "Europe", "region": "Western Europe", "surfacearea": float64(30518), "indepyear": int64(1830), "population": int64(10239000), "lifeexpectancy": float64(77.800003), "gnp": float64(249704), "gnpold": float64(243948), "localname": "België/Belgique", "governmentform": "Constitutional Monarchy, Federation", "headofstate": "Albert II", "capital": int64(179), "code2": "BE"},
				{"code": "DMA", "name": "Dominica", "continent": "North America", "region": "Caribbean", "surfacearea": float64(751), "indepyear": int64(1978), "population": int64(71000), "lifeexpectancy": float64(73.400002), "gnp": float64(256), "gnpold": float64(243), "localname": "Dominica", "governmentform": "Republic", "headofstate": "Vernon Shaw", "capital": int64(586), "code2": "DM"},
				{"code": "GLP", "name": "Guadeloupe", "continent": "North America", "region": "Caribbean", "surfacearea": float64(1705), "indepyear": nil, "population": int64(456000), "lifeexpectancy": float64(77), "gnp": float64(3501), "gnpold": nil, "localname": "Guadeloupe", "governmentform": "Overseas Department of France", "headofstate": "Jacques Chirac", "capital": int64(919), "code2": "GP"},
				{"code": "JAM", "name": "Jamaica", "continent": "North America", "region": "Caribbean", "surfacearea": float64(10990), "indepyear": int64(1962), "population": int64(2583000), "lifeexpectancy": float64(75.199997), "gnp": float64(6871), "gnpold": float64(6722), "localname": "Jamaica", "governmentform": "Constitutional Monarchy", "headofstate": "Elisabeth II", "capital": int64(1530), "code2": "JM"},
				{"code": "CCK", "name": "Cocos (Keeling) Islands", "continent": "Oceania", "region": "Australia and New Zealand", "surfacearea": float64(14), "indepyear": nil, "population": int64(600), "lifeexpectancy": nil, "gnp": float64(0), "gnpold": nil, "localname": "Cocos (Keeling) Islands", "governmentform": "Territory of Australia", "headofstate": "Elisabeth II", "capital": int64(2317), "code2": "CC"},
				{"code": "MKD", "name": "Macedonia", "continent": "Europe", "region": "Southern Europe", "surfacearea": float64(25713), "indepyear": int64(1991), "population": int64(2024000), "lifeexpectancy": float64(73.800003), "gnp": float64(1694), "gnpold": float64(1915), "localname": "Makedonija", "governmentform": "Republic", "headofstate": "Boris Trajkovski", "capital": int64(2460), "code2": "MK"},
				{"code": "NAM", "name": "Namibia", "continent": "Africa", "region": "Southern Africa", "surfacearea": float64(824292), "indepyear": int64(1990), "population": int64(1726000), "lifeexpectancy": float64(42.5), "gnp": float64(3101), "gnpold": float64(3384), "localname": "Namibia", "governmentform": "Republic", "headofstate": "Sam Nujoma", "capital": int64(2726), "code2": "NA"},
				{"code": "PRI", "name": "Puerto Rico", "continent": "North America", "region": "Caribbean", "surfacearea": float64(8875), "indepyear": nil, "population": int64(3869000), "lifeexpectancy": float64(75.599998), "gnp": float64(34100), "gnpold": float64(32100), "localname": "Puerto Rico", "governmentform": "Commonwealth of the US", "headofstate": "George W. Bush", "capital": int64(2919), "code2": "PR"},
				{"code": "SMR", "name": "San Marino", "continent": "Europe", "region": "Southern Europe", "surfacearea": float64(61), "indepyear": int64(885), "population": int64(27000), "lifeexpectancy": float64(81.099998), "gnp": float64(510), "gnpold": nil, "localname": "San Marino", "governmentform": "Republic", "headofstate": nil, "capital": int64(3171), "code2": "SM"},
				{"code": "DNK", "name": "Denmark", "continent": "Europe", "region": "Nordic Countries", "surfacearea": float64(43094), "indepyear": int64(800), "population": int64(5330000), "lifeexpectancy": float64(76.5), "gnp": float64(174099), "gnpold": float64(169264), "localname": "Danmark", "governmentform": "Constitutional Monarchy", "headofstate": "Margrethe II", "capital": int64(3315), "code2": "DK"},
				{"code": "BLR", "name": "Belarus", "continent": "Europe", "region": "Eastern Europe", "surfacearea": float64(207600), "indepyear": int64(1991), "population": int64(10236000), "lifeexpectancy": float64(68), "gnp": float64(13714), "gnpold": nil, "localname": "Belarus", "governmentform": "Republic", "headofstate": "Aljaksandr Luka\u009aenka", "capital": int64(3520), "code2": "BY"},
			}},
			"public.countrylanguage": {BatchSize: 50, Rows: []entities.TableRow{
				{"countrycode": "AFG", "language": "Pashto", "isofficial": true, "percentage": 52.400002},
				{"countrycode": "FJI", "language": "Fijian", "isofficial": true, "percentage": 50.799999},
				{"countrycode": "CCK", "language": "Malay", "isofficial": false, "percentage": float64(0)},
				{"countrycode": "OMN", "language": "Arabic", "isofficial": true, "percentage": 76.699997},
				{"countrycode": "DNK", "language": "Danish", "isofficial": true, "percentage": 93.5},
				{"countrycode": "BGD", "language": "Chakma", "isofficial": false, "percentage": 0.40000001},
				{"countrycode": "IRL", "language": "Irish", "isofficial": true, "percentage": 1.6},
				{"countrycode": "MAR", "language": "Berberi", "isofficial": false, "percentage": float64(33)},
				{"countrycode": "SYC", "language": "English", "isofficial": true, "percentage": 3.8},
				{"countrycode": "AND", "language": "Portuguese", "isofficial": false, "percentage": 10.8},
				{"countrycode": "CAN", "language": "Chinese", "isofficial": false, "percentage": 2.5},
				{"countrycode": "REU", "language": "Comorian", "isofficial": false, "percentage": 2.8},
				{"countrycode": "AZE", "language": "Armenian", "isofficial": false, "percentage": float64(2)},
				{"countrycode": "LBR", "language": "Gio", "isofficial": false, "percentage": 7.9000001},
				{"countrycode": "TKM", "language": "Kazakh", "isofficial": false, "percentage": float64(2)},
				{"countrycode": "COD", "language": "Zande", "isofficial": false, "percentage": 6.0999999},
				{"countrycode": "EST", "language": "Finnish", "isofficial": false, "percentage": 0.69999999},
				{"countrycode": "ROM", "language": "Serbo-Croatian", "isofficial": false, "percentage": 0.1},
				{"countrycode": "DNK", "language": "Norwegian", "isofficial": false, "percentage": 0.30000001},
				{"countrycode": "NGA", "language": "Ijo", "isofficial": false, "percentage": 1.8},
			}},
		},
	},
	"aa8y/postgres-dataset:dellstore": {
		DBName: "dellstore",
		Tables: []entities.SQLTable{
			{
				TableName: "public.categories",
				Columns: []entities.TableColumn{
					{ColumnName: "category", ColumnType: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('categories_category_seq'::regclass)"), ColumnPosition: uint(1)},
					{ColumnName: "categoryname", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(2)},
				},
				Constraints: []entities.TableConstraint{
					{ConstraintType: entities.PrimaryKey, ConstraintName: "categories_pkey", Columns: []string{"category"}},
				},
			}, {
				TableName: "public.cust_hist",
				Columns: []entities.TableColumn{
					{ColumnName: "customerid", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(1)},
					{ColumnName: "orderid", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "prod_id", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(3)},
				},
				ForeignKeys: []entities.ForeignKey{
					{ConstraintName: "fk_cust_hist_customerid", Columns: []string{"customerid"}, ReferencedTable: "public.customers", ReferencedColumns: []string{"customerid"}, UpdateAction: entities.NoAction, DeleteAction: entities.Cascade},
				},
				Indexes: []entities.Index{
					{IndexName: "ix_cust_hist_customerid", IndexType: "btree", Columns: []string{"customerid"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.customers",
				Columns: []entities.TableColumn{
					{ColumnName: "customerid", ColumnType: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('customers_customerid_seq'::regclass)"), ColumnPosition: uint(1)},
					{ColumnName: "firstname", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "lastname", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(3)},
					{ColumnName: "address1", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(4)},
					{ColumnName: "address2", ColumnType: "character varying(50)", IsNullable: true, ColumnPosition: uint(5)},
					{ColumnName: "city", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(6)},
					{ColumnName: "state", ColumnType: "character varying(50)", IsNullable: true, ColumnPosition: uint(7)},
					{ColumnName: "zip", ColumnType: "integer", IsNullable: true, ColumnPosition: uint(8)},
					{ColumnName: "country", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(9)},
					{ColumnName: "region", ColumnType: "smallint", IsNullable: false, ColumnPosition: uint(10)},
					{ColumnName: "email", ColumnType: "character varying(50)", IsNullable: true, ColumnPosition: uint(11)},
					{ColumnName: "phone", ColumnType: "character varying(50)", IsNullable: true, ColumnPosition: uint(12)},
					{ColumnName: "creditcardtype", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(13)},
					{ColumnName: "creditcard", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(14)},
					{ColumnName: "creditcardexpiration", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(15)},
					{ColumnName: "username", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(16)},
					{ColumnName: "password", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(17)},
					{ColumnName: "age", ColumnType: "smallint", IsNullable: true, ColumnPosition: uint(18)},
					{ColumnName: "income", ColumnType: "integer", IsNullable: true, ColumnPosition: uint(19)},
					{ColumnName: "gender", ColumnType: "character varying(1)", IsNullable: true, ColumnPosition: uint(20)},
				},
				Constraints: []entities.TableConstraint{
					{ConstraintType: entities.PrimaryKey, ConstraintName: "customers_pkey", Columns: []string{"customerid"}},
				},
				Indexes: []entities.Index{
					{IndexName: "ix_cust_username", IndexType: "btree", Columns: []string{"username"}, Options: map[string]interface{}{"isUnique": true}},
				},
			}, {
				TableName: "public.inventory",
				Columns: []entities.TableColumn{
					{ColumnName: "prod_id", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(1)},
					{ColumnName: "quan_in_stock", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "sales", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(3)},
				},
				Constraints: []entities.TableConstraint{
					{ConstraintType: entities.PrimaryKey, ConstraintName: "inventory_pkey", Columns: []string{"prod_id"}},
				},
			}, {
				TableName: "public.orderlines",
				Columns: []entities.TableColumn{
					{ColumnName: "orderlineid", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(1)},
					{ColumnName: "orderid", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "prod_id", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(3)},
					{ColumnName: "quantity", ColumnType: "smallint", IsNullable: false, ColumnPosition: uint(4)},
					{ColumnName: "orderdate", ColumnType: "date", IsNullable: false, ColumnPosition: uint(5)},
				},
				ForeignKeys: []entities.ForeignKey{
					{ConstraintName: "fk_orderid", Columns: []string{"orderid"}, ReferencedTable: "public.orders", ReferencedColumns: []string{"orderid"}, UpdateAction: entities.NoAction, DeleteAction: entities.Cascade},
				},
				Indexes: []entities.Index{
					{IndexName: "ix_orderlines_orderid", IndexType: "btree", Columns: []string{"orderid", "orderlineid"}, Options: map[string]interface{}{"isUnique": true}},
				},
			}, {
				TableName: "public.orders",
				Columns: []entities.TableColumn{
					{ColumnName: "orderid", ColumnType: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('orders_orderid_seq'::regclass)"), ColumnPosition: uint(1)},
					{ColumnName: "orderdate", ColumnType: "date", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "customerid", ColumnType: "integer", IsNullable: true, ColumnPosition: uint(3)},
					{ColumnName: "netamount", ColumnType: "numeric(12,2)", IsNullable: false, ColumnPosition: uint(4)},
					{ColumnName: "tax", ColumnType: "numeric(12,2)", IsNullable: false, ColumnPosition: uint(5)},
					{ColumnName: "totalamount", ColumnType: "numeric(12,2)", IsNullable: false, ColumnPosition: uint(6)},
				},
				Constraints: []entities.TableConstraint{
					{ConstraintType: entities.PrimaryKey, ConstraintName: "orders_pkey", Columns: []string{"orderid"}},
				},
				ForeignKeys: []entities.ForeignKey{
					{ConstraintName: "fk_customerid", Columns: []string{"customerid"}, ReferencedTable: "public.customers", ReferencedColumns: []string{"customerid"}, UpdateAction: entities.NoAction, DeleteAction: entities.SetNull},
				},
				Indexes: []entities.Index{
					{IndexName: "ix_order_custid", IndexType: "btree", Columns: []string{"customerid"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.products",
				Columns: []entities.TableColumn{
					{ColumnName: "prod_id", ColumnType: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('products_prod_id_seq'::regclass)"), ColumnPosition: uint(1)},
					{ColumnName: "category", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "title", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(3)},
					{ColumnName: "actor", ColumnType: "character varying(50)", IsNullable: false, ColumnPosition: uint(4)},
					{ColumnName: "price", ColumnType: "numeric(12,2)", IsNullable: false, ColumnPosition: uint(5)},
					{ColumnName: "special", ColumnType: "smallint", IsNullable: true, ColumnPosition: uint(6)},
					{ColumnName: "common_prod_id", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(7)},
				},
				Constraints: []entities.TableConstraint{
					{ConstraintType: entities.PrimaryKey, ConstraintName: "products_pkey", Columns: []string{"prod_id"}},
				},
				Indexes: []entities.Index{
					{IndexName: "ix_prod_category", IndexType: "btree", Columns: []string{"category"}, Options: map[string]interface{}{"isUnique": false}},
					{IndexName: "ix_prod_special", IndexType: "btree", Columns: []string{"special"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.reorder",
				Columns: []entities.TableColumn{
					{ColumnName: "prod_id", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(1)},
					{ColumnName: "date_low", ColumnType: "date", IsNullable: false, ColumnPosition: uint(2)},
					{ColumnName: "quan_low", ColumnType: "integer", IsNullable: false, ColumnPosition: uint(3)},
					{ColumnName: "date_reordered", ColumnType: "date", IsNullable: true, ColumnPosition: uint(4)},
					{ColumnName: "quan_reordered", ColumnType: "integer", IsNullable: true, ColumnPosition: uint(5)},
					{ColumnName: "date_expected", ColumnType: "date", IsNullable: true, ColumnPosition: uint(6)},
				},
			},
		},
		Sequences: []entities.PSQLSequence{
			{SequenceName: "public.categories_category_seq", DataType: "integer", StartValue: uint(1), MinimumValue: uint(1), MaximumValue: uint(2147483647), Increment: uint(1), CycleOption: false, LastValue: uint(16), IsCalled: true},
			{SequenceName: "public.customers_customerid_seq", DataType: "integer", StartValue: uint(1), MinimumValue: uint(1), MaximumValue: uint(2147483647), Increment: uint(1), CycleOption: false, LastValue: uint(20000), IsCalled: true},
			{SequenceName: "public.orders_orderid_seq", DataType: "integer", StartValue: uint(1), MinimumValue: uint(1), MaximumValue: uint(2147483647), Increment: uint(1), CycleOption: false, LastValue: uint(12000), IsCalled: true},
			{SequenceName: "public.products_prod_id_seq", DataType: "integer", StartValue: uint(1), MinimumValue: uint(1), MaximumValue: uint(2147483647), Increment: uint(1), CycleOption: false, LastValue: uint(10000), IsCalled: true},
		},
		TableContent: map[string]DBSQLTableContent{
			"public.categories": {BatchSize: 4, Rows: []entities.TableRow{
				{"category": int64(1), "categoryname": "Action"},
				{"category": int64(5), "categoryname": "Comedy"},
				{"category": int64(9), "categoryname": "Foreign"},
				{"category": int64(13), "categoryname": "New"},
			}},
			"public.customers": {BatchSize: 2000, Rows: []entities.TableRow{
				{"customerid": int64(1), "firstname": "VKUUXF", "lastname": "ITHOMQJNYX", "address1": "4608499546 Dell Way", "address2": nil, "city": "QSDPAGD", "state": "SD", "zip": int64(24101), "country": "US", "region": int64(1), "email": "ITHOMQJNYX@dell.com", "phone": "4608499546", "creditcardtype": int64(1), "creditcard": "1979279217775911", "creditcardexpiration": "2012/03", "username": "user1", "password": "password", "age": int64(55), "income": int64(100000), "gender": "M"},
				{"customerid": int64(2001), "firstname": "LTMAHF", "lastname": "LNNBYHHIGY", "address1": "1001614924 Dell Way", "address2": nil, "city": "XTLGXRX", "state": "MD", "zip": int64(60295), "country": "US", "region": int64(1), "email": "LNNBYHHIGY@dell.com", "phone": "1001614924", "creditcardtype": int64(2), "creditcard": "4158183055929840", "creditcardexpiration": "2011/09", "username": "user2001", "password": "password", "age": int64(74), "income": int64(20000), "gender": "F"},
				{"customerid": int64(4001), "firstname": "IMYAYU", "lastname": "YTJXGCFHWJ", "address1": "4942974925 Dell Way", "address2": nil, "city": "EVYIIYY", "state": "LA", "zip": int64(87912), "country": "US", "region": int64(1), "email": "YTJXGCFHWJ@dell.com", "phone": "4942974925", "creditcardtype": int64(2), "creditcard": "3971776042133570", "creditcardexpiration": "2010/01", "username": "user4001", "password": "password", "age": int64(22), "income": int64(100000), "gender": "M"},
				{"customerid": int64(6001), "firstname": "RZKRNQ", "lastname": "ZRDXBZHLZM", "address1": "4214268075 Dell Way", "address2": nil, "city": "KBTHSEF", "state": "ND", "zip": int64(23997), "country": "US", "region": int64(1), "email": "ZRDXBZHLZM@dell.com", "phone": "4214268075", "creditcardtype": int64(2), "creditcard": "7078812733292580", "creditcardexpiration": "2010/11", "username": "user6001", "password": "password", "age": int64(70), "income": int64(40000), "gender": "M"},
				{"customerid": int64(8001), "firstname": "RTYKLZ", "lastname": "AWHGWKNKMF", "address1": "9094764851 Dell Way", "address2": nil, "city": "LAZWJQW", "state": "WY", "zip": int64(34834), "country": "US", "region": int64(1), "email": "AWHGWKNKMF@dell.com", "phone": "9094764851", "creditcardtype": int64(5), "creditcard": "5071015951689333", "creditcardexpiration": "2008/04", "username": "user8001", "password": "password", "age": int64(38), "income": int64(80000), "gender": "F"},
				{"customerid": int64(10001), "firstname": "QQGWSL", "lastname": "NNTHRFAVRX", "address1": "6739733410 Dell Way", "address2": nil, "city": "KQLKYDL", "state": nil, "zip": int64(0), "country": "UK", "region": int64(2), "email": "NNTHRFAVRX@dell.com", "phone": "6739733410", "creditcardtype": int64(5), "creditcard": "3180290846154094", "creditcardexpiration": "2010/06", "username": "user10001", "password": "password", "age": int64(62), "income": int64(100000), "gender": "F"},
				{"customerid": int64(12001), "firstname": "KKPQBY", "lastname": "JDLUPSWWSD", "address1": "7087720097 Dell Way", "address2": nil, "city": "FMJQJVB", "state": nil, "zip": int64(0), "country": "Chile", "region": int64(2), "email": "JDLUPSWWSD@dell.com", "phone": "7087720097", "creditcardtype": int64(5), "creditcard": "6277906919363887", "creditcardexpiration": "2011/11", "username": "user12001", "password": "password", "age": int64(63), "income": int64(20000), "gender": "F"},
				{"customerid": int64(14001), "firstname": "BPGIXX", "lastname": "XWRAGPSDPR", "address1": "6341127964 Dell Way", "address2": nil, "city": "HCVCEGB", "state": nil, "zip": int64(0), "country": "Germany", "region": int64(2), "email": "XWRAGPSDPR@dell.com", "phone": "6341127964", "creditcardtype": int64(5), "creditcard": "2779878668952733", "creditcardexpiration": "2012/05", "username": "user14001", "password": "password", "age": int64(41), "income": int64(80000), "gender": "F"},
				{"customerid": int64(16001), "firstname": "NLNNKJ", "lastname": "OFPQTZQNZG", "address1": "9248454082 Dell Way", "address2": nil, "city": "UXEKOQN", "state": nil, "zip": int64(0), "country": "Australia", "region": int64(2), "email": "OFPQTZQNZG@dell.com", "phone": "9248454082", "creditcardtype": int64(3), "creditcard": "3312776351813227", "creditcardexpiration": "2009/08", "username": "user16001", "password": "password", "age": int64(75), "income": int64(40000), "gender": "F"},
				{"customerid": int64(18001), "firstname": "XWYOVV", "lastname": "OFBFKFIRLM", "address1": "2933766882 Dell Way", "address2": nil, "city": "ONXARPP", "state": nil, "zip": int64(0), "country": "Canada", "region": int64(2), "email": "OFBFKFIRLM@dell.com", "phone": "2933766882", "creditcardtype": int64(3), "creditcard": "1694544910918921", "creditcardexpiration": "2011/07", "username": "user18001", "password": "password", "age": int64(67), "income": int64(20000), "gender": "F"},
			}},
			"public.cust_hist": {BatchSize: 10000, Rows: []entities.TableRow{
				{"customerid": int64(7888), "orderid": int64(1), "prod_id": int64(9117)},
				{"customerid": int64(12033), "orderid": int64(1985), "prod_id": int64(3844)},
				{"customerid": int64(13953), "orderid": int64(3958), "prod_id": int64(7382)},
				{"customerid": int64(7638), "orderid": int64(5946), "prod_id": int64(6664)},
				{"customerid": int64(9534), "orderid": int64(7947), "prod_id": int64(7549)},
				{"customerid": int64(760), "orderid": int64(9925), "prod_id": int64(1585)},
				{"customerid": int64(11438), "orderid": int64(11927), "prod_id": int64(948)},
			}},
			"public.inventory": {BatchSize: 2000, Rows: []entities.TableRow{
				{"prod_id": int64(1), "quan_in_stock": int64(138), "sales": int64(9)},
				{"prod_id": int64(2001), "quan_in_stock": int64(235), "sales": int64(6)},
				{"prod_id": int64(4001), "quan_in_stock": int64(163), "sales": int64(15)},
				{"prod_id": int64(6001), "quan_in_stock": int64(218), "sales": int64(17)},
				{"prod_id": int64(8001), "quan_in_stock": int64(152), "sales": int64(2)},
			}},
			"public.orderlines": {BatchSize: 10000, Rows: []entities.TableRow{
				{"orderlineid": int64(1), "orderid": int64(1), "prod_id": int64(9117), "quantity": int64(1), "orderdate": time.Date(2004, 1, 27, 0, 0, 0, 0, time.UTC)},
				{"orderlineid": int64(2), "orderid": int64(1985), "prod_id": int64(3844), "quantity": int64(3), "orderdate": time.Date(2004, 2, 28, 0, 0, 0, 0, time.UTC)},
				{"orderlineid": int64(4), "orderid": int64(3958), "prod_id": int64(7382), "quantity": int64(3), "orderdate": time.Date(2004, 4, 15, 0, 0, 0, 0, time.UTC)},
				{"orderlineid": int64(6), "orderid": int64(5946), "prod_id": int64(6664), "quantity": int64(2), "orderdate": time.Date(2004, 6, 18, 0, 0, 0, 0, time.UTC)},
				{"orderlineid": int64(2), "orderid": int64(7947), "prod_id": int64(7549), "quantity": int64(3), "orderdate": time.Date(2004, 8, 6, 0, 0, 0, 0, time.UTC)},
				{"orderlineid": int64(1), "orderid": int64(9925), "prod_id": int64(1585), "quantity": int64(1), "orderdate": time.Date(2004, 10, 31, 0, 0, 0, 0, time.UTC)},
				{"orderlineid": int64(7), "orderid": int64(11927), "prod_id": int64(948), "quantity": int64(3), "orderdate": time.Date(2004, 12, 25, 0, 0, 0, 0, time.UTC)},
			}},
			"public.orders": {BatchSize: 4000, Rows: []entities.TableRow{
				{"orderid": int64(1), "orderdate": time.Date(2004, 1, 27, 0, 0, 0, 0, time.UTC), "customerid": int64(7888), "netamount": float64(313.24), "tax": float64(25.84), "totalamount": float64(339.08)},
				{"orderid": int64(4001), "orderdate": time.Date(2004, 5, 12, 0, 0, 0, 0, time.UTC), "customerid": int64(14005), "netamount": float64(350.6), "tax": float64(28.92), "totalamount": float64(379.52)},
				{"orderid": int64(8001), "orderdate": time.Date(2004, 9, 12, 0, 0, 0, 0, time.UTC), "customerid": int64(9922), "netamount": float64(384.52), "tax": float64(31.72), "totalamount": float64(416.24)},
			}},
			"public.products": {BatchSize: 2000, Rows: []entities.TableRow{
				{"prod_id": int64(1), "category": int64(14), "title": "ACADEMY ACADEMY", "actor": "PENELOPE GUINESS", "price": float64(25.99), "special": int64(0), "common_prod_id": int64(1976)},
				{"prod_id": int64(2001), "category": int64(5), "title": "ADAPTATION ACADEMY", "actor": "CARY NEESON", "price": float64(19.99), "special": int64(0), "common_prod_id": int64(3385)},
				{"prod_id": int64(4001), "category": int64(11), "title": "AFRICAN ACADEMY", "actor": "INGRID DOUGLAS", "price": float64(20.99), "special": int64(0), "common_prod_id": int64(9099)},
				{"prod_id": int64(6001), "category": int64(1), "title": "AIRPLANE ACADEMY", "actor": "TIM BRIDGES", "price": float64(22.99), "special": int64(0), "common_prod_id": int64(5905)},
				{"prod_id": int64(8001), "category": int64(13), "title": "ALABAMA ACADEMY", "actor": "ANNETTE KINNEAR", "price": float64(11.99), "special": int64(0), "common_prod_id": int64(2439)},
			}},
			"public.reorder": {BatchSize: 1},
		},
	},
}

func runPostgreSQLTests(t *testing.T) {
	for image, data := range imageData {
		db, cleanup := setupPostgreSQLContainer(t, image, data.DBName)

		dbReader := database_impl.NewPostgreSQLDatabaseReader(db)
		testListSchemasDefinition(t, dbReader, data)
		testGetDatabaseExtraInfo(t, dbReader, data)
		testGetSchemaDataBatch(t, dbReader, data)

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

func testListSchemasDefinition(t *testing.T, dbReader services.DatabaseReader, expectedData DBSQLTestContent) {
	schemas, err := dbReader.ListSchemasDefinition()
	assert.Nil(t, err)
	assert.NotNil(t, schemas)
	assert.Equal(t, len(expectedData.Tables), len(schemas))

	expectedTables := expectedData.Tables
	for i, schema := range schemas {
		table := schema.(*entities.SQLTable)

		assert.Equal(t, expectedTables[i].TableName, table.TableName)
		assert.Equal(t, len(expectedTables[i].Columns), len(table.Columns))
		assert.Equal(t, len(expectedTables[i].Constraints), len(table.Constraints))
		assert.Equal(t, len(expectedTables[i].ForeignKeys), len(table.ForeignKeys))
		assert.Equal(t, len(expectedTables[i].Indexes), len(table.Indexes))

		expectedColumns := expectedTables[i].Columns
		for j, column := range table.Columns {
			assert.Equal(t, expectedColumns[j].ColumnName, column.ColumnName)
			assert.Equal(t, expectedColumns[j].ColumnType, column.ColumnType)
			assert.Equal(t, expectedColumns[j].IsNullable, column.IsNullable)
			assert.Equal(t, expectedColumns[j].DefaultValue, column.DefaultValue)
			assert.Equal(t, expectedColumns[j].ColumnPosition, column.ColumnPosition)
		}

		expectedConstraints := expectedTables[i].Constraints
		for j, constraint := range table.Constraints {
			assert.Equal(t, expectedConstraints[j].ConstraintType, constraint.ConstraintType)
			assert.Equal(t, expectedConstraints[j].ConstraintName, constraint.ConstraintName)
			assert.Equal(t, expectedConstraints[j].Columns, constraint.Columns)
			assert.Equal(t, expectedConstraints[j].Definition, constraint.Definition)
		}

		expectedForeignKeys := expectedTables[i].ForeignKeys
		for j, fKey := range table.ForeignKeys {
			assert.Equal(t, expectedForeignKeys[j].ConstraintName, fKey.ConstraintName)
			assert.Equal(t, expectedForeignKeys[j].Columns, fKey.Columns)
			assert.Equal(t, expectedForeignKeys[j].ReferencedTable, fKey.ReferencedTable)
			assert.Equal(t, expectedForeignKeys[j].ReferencedColumns, fKey.ReferencedColumns)
			assert.Equal(t, expectedForeignKeys[j].UpdateAction, fKey.UpdateAction)
			assert.Equal(t, expectedForeignKeys[j].DeleteAction, fKey.DeleteAction)
		}

		expectedIndexes := expectedTables[i].Indexes
		for j, index := range table.Indexes {
			assert.Equal(t, expectedIndexes[j].IndexName, index.IndexName)
			assert.Equal(t, expectedIndexes[j].IndexType, index.IndexType)
			assert.Equal(t, expectedIndexes[j].Columns, index.Columns)
			assert.Equal(t, expectedIndexes[j].Options, index.Options)
		}
	}
}

func testGetDatabaseExtraInfo(t *testing.T, dbReader services.DatabaseReader, expectedData DBSQLTestContent) {
	extraInfo, err := dbReader.GetDatabaseExtraInfo()
	assert.Nil(t, err)
	assert.NotNil(t, extraInfo)

	info := extraInfo.(*entities.PSQLDBExtraInfo)
	assert.Equal(t, len(expectedData.Sequences), len(info.Sequences))

	expectedSequences := expectedData.Sequences
	for i, sequence := range info.Sequences {
		assert.Equal(t, expectedSequences[i].SequenceName, sequence.SequenceName)
		assert.Equal(t, expectedSequences[i].DataType, sequence.DataType)
		assert.Equal(t, expectedSequences[i].StartValue, sequence.StartValue)
		assert.Equal(t, expectedSequences[i].MinimumValue, sequence.MinimumValue)
		assert.Equal(t, expectedSequences[i].MaximumValue, sequence.MaximumValue)
		assert.Equal(t, expectedSequences[i].Increment, sequence.Increment)
		assert.Equal(t, expectedSequences[i].CycleOption, sequence.CycleOption)
		assert.Equal(t, expectedSequences[i].LastValue, sequence.LastValue)
		assert.Equal(t, expectedSequences[i].IsCalled, sequence.IsCalled)
	}
}

func testGetSchemaDataBatch(t *testing.T, dbReader services.DatabaseReader, expectedData DBSQLTestContent) {
	schemas, err := dbReader.ListSchemasDefinition()
	assert.Nil(t, err)
	assert.NotNil(t, schemas)

	for _, schema := range schemas {
		table := schema.(*entities.SQLTable)

		expectedRows := expectedData.TableContent[table.TableName].Rows
		var cursor services.BatchCursor = nil
		expectedRowIndex := 0
		for {
			rows, nextCursor, err := dbReader.GetSchemaDataBatch(table, expectedData.TableContent[table.TableName].BatchSize, cursor)
			assert.Nil(t, err)

			if len(rows) == 0 {
				break
			} else {
				assert.LessOrEqual(t, uint(len(rows)), expectedData.TableContent[table.TableName].BatchSize)
				for key, value := range expectedRows[expectedRowIndex] {
					switch a := value.(type) {
					case time.Time:
						e, ok := rows[0].(entities.TableRow)[key].(time.Time)
						assert.True(t, ok)
						assert.Equal(t, e.Unix(), a.Unix())
					default:
						assert.Equal(t, value, rows[0].(entities.TableRow)[key])
					}
				}
				expectedRowIndex++
			}

			cursor = nextCursor
		}
	}
}
