package test

import (
	"context"
	"database/sql"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/helpers"
	"historydb/src/internal/services"
	"historydb/src/internal/services/database_impl"
	serv_entities "historydb/src/internal/services/entities"
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
		Tables: []serv_entities.SQLTable{
			{
				TableName: "public.city",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "name", Type: "text", IsNullable: false, Position: 2},
					{Name: "countrycode", Type: "character(3)", IsNullable: false, Position: 3},
					{Name: "district", Type: "text", IsNullable: false, Position: 4},
					{Name: "population", Type: "integer", IsNullable: false, Position: 5},
				},
				Constraints: []serv_entities.SQLTableConstraint{
					{Type: serv_entities.PrimaryKey, Name: "city_pkey", Columns: []string{"id"}},
				},
			}, {
				TableName: "public.country",
				Columns: []serv_entities.SQLTableColumn{
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
				Constraints: []serv_entities.SQLTableConstraint{
					{Type: serv_entities.Check, Name: "country_continent_check", Definition: helpers.Pointer("((continent = 'Asia'::text) OR (continent = 'Europe'::text) OR (continent = 'North America'::text) OR (continent = 'Africa'::text) OR (continent = 'Oceania'::text) OR (continent = 'Antarctica'::text) OR (continent = 'South America'::text))")},
					{Type: serv_entities.PrimaryKey, Name: "country_pkey", Columns: []string{"code"}},
				},
				ForeignKeys: []serv_entities.SQLTableForeignKey{
					{Name: "country_capital_fkey", Columns: []string{"capital"}, ReferencedTable: "public.city", ReferencedColumns: []string{"id"}, UpdateAction: serv_entities.NoAction, DeleteAction: serv_entities.NoAction},
				},
			}, {
				TableName: "public.countrylanguage",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "countrycode", Type: "character(3)", IsNullable: false, Position: 1},
					{Name: "language", Type: "text", IsNullable: false, Position: 2},
					{Name: "isofficial", Type: "boolean", IsNullable: false, Position: 3},
					{Name: "percentage", Type: "real", IsNullable: false, Position: 4},
				},
				Constraints: []serv_entities.SQLTableConstraint{
					{Type: serv_entities.PrimaryKey, Name: "countrylanguage_pkey", Columns: []string{"countrycode", "language"}},
				},
				ForeignKeys: []serv_entities.SQLTableForeignKey{
					{Name: "countrylanguage_countrycode_fkey", Columns: []string{"countrycode"}, ReferencedTable: "public.country", ReferencedColumns: []string{"code"}, UpdateAction: serv_entities.NoAction, DeleteAction: serv_entities.NoAction},
				},
			},
		},
		TableContent: map[string]DBSQLTableContent{
			"public.city": {DataLength: 4079, ChunkSize: 200, Rows: []serv_entities.TableRow{
				{{Column: "id", Value: int64(1)}, {Column: "name", Value: "Kabul"}, {Column: "countrycode", Value: "AFG"}, {Column: "district", Value: "Kabol"}, {Column: "population", Value: int64(1780000)}},
				{{Column: "id", Value: int64(201)}, {Column: "name", Value: "Sarajevo"}, {Column: "countrycode", Value: "BIH"}, {Column: "district", Value: "Federaatio"}, {Column: "population", Value: int64(360000)}},
				{{Column: "id", Value: int64(401)}, {Column: "name", Value: "Ribeirão Pires"}, {Column: "countrycode", Value: "BRA"}, {Column: "district", Value: "São Paulo"}, {Column: "population", Value: int64(108121)}},
				{{Column: "id", Value: int64(601)}, {Column: "name", Value: "Duran [Eloy Alfaro]"}, {Column: "countrycode", Value: "ECU"}, {Column: "district", Value: "Guayas"}, {Column: "population", Value: int64(152514)}},
				{{Column: "id", Value: int64(801)}, {Column: "name", Value: "Cabanatuan"}, {Column: "countrycode", Value: "PHL"}, {Column: "district", Value: "Central Luzon"}, {Column: "population", Value: int64(222859)}},
				{{Column: "id", Value: int64(1001)}, {Column: "name", Value: "Depok"}, {Column: "countrycode", Value: "IDN"}, {Column: "district", Value: "Yogyakarta"}, {Column: "population", Value: int64(106800)}},
				{{Column: "id", Value: int64(1201)}, {Column: "name", Value: "Cuddalore"}, {Column: "countrycode", Value: "IND"}, {Column: "district", Value: "Tamil Nadu"}, {Column: "population", Value: int64(153086)}},
				{{Column: "id", Value: int64(1401)}, {Column: "name", Value: "Khorramabad"}, {Column: "countrycode", Value: "IRN"}, {Column: "district", Value: "Lorestan"}, {Column: "population", Value: int64(272815)}},
				{{Column: "id", Value: int64(1601)}, {Column: "name", Value: "Yokkaichi"}, {Column: "countrycode", Value: "JPN"}, {Column: "district", Value: "Mie"}, {Column: "population", Value: int64(288173)}},
				{{Column: "id", Value: int64(1801)}, {Column: "name", Value: "Battambang"}, {Column: "countrycode", Value: "KHM"}, {Column: "district", Value: "Battambang"}, {Column: "population", Value: int64(129800)}},
				{{Column: "id", Value: int64(2001)}, {Column: "name", Value: "Tengzhou"}, {Column: "countrycode", Value: "CHN"}, {Column: "district", Value: "Shandong"}, {Column: "population", Value: int64(315083)}},
				{{Column: "id", Value: int64(2201)}, {Column: "name", Value: "Kaili"}, {Column: "countrycode", Value: "CHN"}, {Column: "district", Value: "Guizhou"}, {Column: "population", Value: int64(113958)}},
				{{Column: "id", Value: int64(2401)}, {Column: "name", Value: "Athenai"}, {Column: "countrycode", Value: "GRC"}, {Column: "district", Value: "Attika"}, {Column: "population", Value: int64(772072)}},
				{{Column: "id", Value: int64(2601)}, {Column: "name", Value: "Tecámac"}, {Column: "countrycode", Value: "MEX"}, {Column: "district", Value: "México"}, {Column: "population", Value: int64(172410)}},
				{{Column: "id", Value: int64(2801)}, {Column: "name", Value: "Agege"}, {Column: "countrycode", Value: "NGA"}, {Column: "district", Value: "Lagos"}, {Column: "population", Value: int64(105000)}},
				{{Column: "id", Value: int64(3001)}, {Column: "name", Value: "Metz"}, {Column: "countrycode", Value: "FRA"}, {Column: "district", Value: "Lorraine"}, {Column: "population", Value: int64(123776)}},
				{{Column: "id", Value: int64(3201)}, {Column: "name", Value: "Ziguinchor"}, {Column: "countrycode", Value: "SEN"}, {Column: "district", Value: "Ziguinchor"}, {Column: "population", Value: int64(192000)}},
				{{Column: "id", Value: int64(3401)}, {Column: "name", Value: "Kilis"}, {Column: "countrycode", Value: "TUR"}, {Column: "district", Value: "Kilis"}, {Column: "population", Value: int64(118245)}},
				{{Column: "id", Value: int64(3601)}, {Column: "name", Value: "Habarovsk"}, {Column: "countrycode", Value: "RUS"}, {Column: "district", Value: "Habarovsk"}, {Column: "population", Value: int64(609400)}},
				{{Column: "id", Value: int64(3801)}, {Column: "name", Value: "San Antonio"}, {Column: "countrycode", Value: "USA"}, {Column: "district", Value: "Texas"}, {Column: "population", Value: int64(1144646)}},
				{{Column: "id", Value: int64(4001)}, {Column: "name", Value: "Gilbert"}, {Column: "countrycode", Value: "USA"}, {Column: "district", Value: "Arizona"}, {Column: "population", Value: int64(109697)}},
			}},
			"public.country": {DataLength: 239, ChunkSize: 20, Rows: []serv_entities.TableRow{
				{{Column: "code", Value: "AFG"}, {Column: "name", Value: "Afghanistan"}, {Column: "continent", Value: "Asia"}, {Column: "region", Value: "Southern and Central Asia"}, {Column: "surfacearea", Value: float64(652090)}, {Column: "indepyear", Value: int64(1919)}, {Column: "population", Value: int64(22720000)}, {Column: "lifeexpectancy", Value: float64(45.900002)}, {Column: "gnp", Value: float64(5976)}, {Column: "gnpold", Value: nil}, {Column: "localname", Value: "Afganistan/Afqanestan"}, {Column: "governmentform", Value: "Islamic Emirate"}, {Column: "headofstate", Value: "Mohammad Omar"}, {Column: "capital", Value: int64(1)}, {Column: "code2", Value: "AF"}},
				{{Column: "code", Value: "BEL"}, {Column: "name", Value: "Belgium"}, {Column: "continent", Value: "Europe"}, {Column: "region", Value: "Western Europe"}, {Column: "surfacearea", Value: float64(30518)}, {Column: "indepyear", Value: int64(1830)}, {Column: "population", Value: int64(10239000)}, {Column: "lifeexpectancy", Value: float64(77.800003)}, {Column: "gnp", Value: float64(249704)}, {Column: "gnpold", Value: float64(243948)}, {Column: "localname", Value: "België/Belgique"}, {Column: "governmentform", Value: "Constitutional Monarchy, Federation"}, {Column: "headofstate", Value: "Albert II"}, {Column: "capital", Value: int64(179)}, {Column: "code2", Value: "BE"}},
				{{Column: "code", Value: "DMA"}, {Column: "name", Value: "Dominica"}, {Column: "continent", Value: "North America"}, {Column: "region", Value: "Caribbean"}, {Column: "surfacearea", Value: float64(751)}, {Column: "indepyear", Value: int64(1978)}, {Column: "population", Value: int64(71000)}, {Column: "lifeexpectancy", Value: float64(73.400002)}, {Column: "gnp", Value: float64(256)}, {Column: "gnpold", Value: float64(243)}, {Column: "localname", Value: "Dominica"}, {Column: "governmentform", Value: "Republic"}, {Column: "headofstate", Value: "Vernon Shaw"}, {Column: "capital", Value: int64(586)}, {Column: "code2", Value: "DM"}},
				{{Column: "code", Value: "GLP"}, {Column: "name", Value: "Guadeloupe"}, {Column: "continent", Value: "North America"}, {Column: "region", Value: "Caribbean"}, {Column: "surfacearea", Value: float64(1705)}, {Column: "indepyear", Value: nil}, {Column: "population", Value: int64(456000)}, {Column: "lifeexpectancy", Value: float64(77)}, {Column: "gnp", Value: float64(3501)}, {Column: "gnpold", Value: nil}, {Column: "localname", Value: "Guadeloupe"}, {Column: "governmentform", Value: "Overseas Department of France"}, {Column: "headofstate", Value: "Jacques Chirac"}, {Column: "capital", Value: int64(919)}, {Column: "code2", Value: "GP"}},
				{{Column: "code", Value: "JAM"}, {Column: "name", Value: "Jamaica"}, {Column: "continent", Value: "North America"}, {Column: "region", Value: "Caribbean"}, {Column: "surfacearea", Value: float64(10990)}, {Column: "indepyear", Value: int64(1962)}, {Column: "population", Value: int64(2583000)}, {Column: "lifeexpectancy", Value: float64(75.199997)}, {Column: "gnp", Value: float64(6871)}, {Column: "gnpold", Value: float64(6722)}, {Column: "localname", Value: "Jamaica"}, {Column: "governmentform", Value: "Constitutional Monarchy"}, {Column: "headofstate", Value: "Elisabeth II"}, {Column: "capital", Value: int64(1530)}, {Column: "code2", Value: "JM"}},
				{{Column: "code", Value: "CCK"}, {Column: "name", Value: "Cocos (Keeling) Islands"}, {Column: "continent", Value: "Oceania"}, {Column: "region", Value: "Australia and New Zealand"}, {Column: "surfacearea", Value: float64(14)}, {Column: "indepyear", Value: nil}, {Column: "population", Value: int64(600)}, {Column: "lifeexpectancy", Value: nil}, {Column: "gnp", Value: float64(0)}, {Column: "gnpold", Value: nil}, {Column: "localname", Value: "Cocos (Keeling) Islands"}, {Column: "governmentform", Value: "Territory of Australia"}, {Column: "headofstate", Value: "Elisabeth II"}, {Column: "capital", Value: int64(2317)}, {Column: "code2", Value: "CC"}},
				{{Column: "code", Value: "MKD"}, {Column: "name", Value: "Macedonia"}, {Column: "continent", Value: "Europe"}, {Column: "region", Value: "Southern Europe"}, {Column: "surfacearea", Value: float64(25713)}, {Column: "indepyear", Value: int64(1991)}, {Column: "population", Value: int64(2024000)}, {Column: "lifeexpectancy", Value: float64(73.800003)}, {Column: "gnp", Value: float64(1694)}, {Column: "gnpold", Value: float64(1915)}, {Column: "localname", Value: "Makedonija"}, {Column: "governmentform", Value: "Republic"}, {Column: "headofstate", Value: "Boris Trajkovski"}, {Column: "capital", Value: int64(2460)}, {Column: "code2", Value: "MK"}},
				{{Column: "code", Value: "NAM"}, {Column: "name", Value: "Namibia"}, {Column: "continent", Value: "Africa"}, {Column: "region", Value: "Southern Africa"}, {Column: "surfacearea", Value: float64(824292)}, {Column: "indepyear", Value: int64(1990)}, {Column: "population", Value: int64(1726000)}, {Column: "lifeexpectancy", Value: float64(42.5)}, {Column: "gnp", Value: float64(3101)}, {Column: "gnpold", Value: float64(3384)}, {Column: "localname", Value: "Namibia"}, {Column: "governmentform", Value: "Republic"}, {Column: "headofstate", Value: "Sam Nujoma"}, {Column: "capital", Value: int64(2726)}, {Column: "code2", Value: "NA"}},
				{{Column: "code", Value: "PRI"}, {Column: "name", Value: "Puerto Rico"}, {Column: "continent", Value: "North America"}, {Column: "region", Value: "Caribbean"}, {Column: "surfacearea", Value: float64(8875)}, {Column: "indepyear", Value: nil}, {Column: "population", Value: int64(3869000)}, {Column: "lifeexpectancy", Value: float64(75.599998)}, {Column: "gnp", Value: float64(34100)}, {Column: "gnpold", Value: float64(32100)}, {Column: "localname", Value: "Puerto Rico"}, {Column: "governmentform", Value: "Commonwealth of the US"}, {Column: "headofstate", Value: "George W. Bush"}, {Column: "capital", Value: int64(2919)}, {Column: "code2", Value: "PR"}},
				{{Column: "code", Value: "SMR"}, {Column: "name", Value: "San Marino"}, {Column: "continent", Value: "Europe"}, {Column: "region", Value: "Southern Europe"}, {Column: "surfacearea", Value: float64(61)}, {Column: "indepyear", Value: int64(885)}, {Column: "population", Value: int64(27000)}, {Column: "lifeexpectancy", Value: float64(81.099998)}, {Column: "gnp", Value: float64(510)}, {Column: "gnpold", Value: nil}, {Column: "localname", Value: "San Marino"}, {Column: "governmentform", Value: "Republic"}, {Column: "headofstate", Value: nil}, {Column: "capital", Value: int64(3171)}, {Column: "code2", Value: "SM"}},
				{{Column: "code", Value: "DNK"}, {Column: "name", Value: "Denmark"}, {Column: "continent", Value: "Europe"}, {Column: "region", Value: "Nordic Countries"}, {Column: "surfacearea", Value: float64(43094)}, {Column: "indepyear", Value: int64(800)}, {Column: "population", Value: int64(5330000)}, {Column: "lifeexpectancy", Value: float64(76.5)}, {Column: "gnp", Value: float64(174099)}, {Column: "gnpold", Value: float64(169264)}, {Column: "localname", Value: "Danmark"}, {Column: "governmentform", Value: "Constitutional Monarchy"}, {Column: "headofstate", Value: "Margrethe II"}, {Column: "capital", Value: int64(3315)}, {Column: "code2", Value: "DK"}},
				{{Column: "code", Value: "BLR"}, {Column: "name", Value: "Belarus"}, {Column: "continent", Value: "Europe"}, {Column: "region", Value: "Eastern Europe"}, {Column: "surfacearea", Value: float64(207600)}, {Column: "indepyear", Value: int64(1991)}, {Column: "population", Value: int64(10236000)}, {Column: "lifeexpectancy", Value: float64(68)}, {Column: "gnp", Value: float64(13714)}, {Column: "gnpold", Value: nil}, {Column: "localname", Value: "Belarus"}, {Column: "governmentform", Value: "Republic"}, {Column: "headofstate", Value: "Aljaksandr Luka\u009aenka"}, {Column: "capital", Value: int64(3520)}, {Column: "code2", Value: "BY"}},
			}},
			"public.countrylanguage": {DataLength: 984, ChunkSize: 50, Rows: []serv_entities.TableRow{
				{{Column: "countrycode", Value: "AFG"}, {Column: "language", Value: "Pashto"}, {Column: "isofficial", Value: true}, {Column: "percentage", Value: 52.400002}},
				{{Column: "countrycode", Value: "FJI"}, {Column: "language", Value: "Fijian"}, {Column: "isofficial", Value: true}, {Column: "percentage", Value: 50.799999}},
				{{Column: "countrycode", Value: "CCK"}, {Column: "language", Value: "Malay"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: float64(0)}},
				{{Column: "countrycode", Value: "OMN"}, {Column: "language", Value: "Arabic"}, {Column: "isofficial", Value: true}, {Column: "percentage", Value: 76.699997}},
				{{Column: "countrycode", Value: "DNK"}, {Column: "language", Value: "Danish"}, {Column: "isofficial", Value: true}, {Column: "percentage", Value: 93.5}},
				{{Column: "countrycode", Value: "BGD"}, {Column: "language", Value: "Chakma"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 0.40000001}},
				{{Column: "countrycode", Value: "IRL"}, {Column: "language", Value: "Irish"}, {Column: "isofficial", Value: true}, {Column: "percentage", Value: 1.6}},
				{{Column: "countrycode", Value: "MAR"}, {Column: "language", Value: "Berberi"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: float64(33)}},
				{{Column: "countrycode", Value: "SYC"}, {Column: "language", Value: "English"}, {Column: "isofficial", Value: true}, {Column: "percentage", Value: 3.8}},
				{{Column: "countrycode", Value: "AND"}, {Column: "language", Value: "Portuguese"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 10.8}},
				{{Column: "countrycode", Value: "CAN"}, {Column: "language", Value: "Chinese"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 2.5}},
				{{Column: "countrycode", Value: "REU"}, {Column: "language", Value: "Comorian"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 2.8}},
				{{Column: "countrycode", Value: "AZE"}, {Column: "language", Value: "Armenian"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: float64(2)}},
				{{Column: "countrycode", Value: "LBR"}, {Column: "language", Value: "Gio"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 7.9000001}},
				{{Column: "countrycode", Value: "TKM"}, {Column: "language", Value: "Kazakh"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: float64(2)}},
				{{Column: "countrycode", Value: "COD"}, {Column: "language", Value: "Zande"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 6.0999999}},
				{{Column: "countrycode", Value: "EST"}, {Column: "language", Value: "Finnish"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 0.69999999}},
				{{Column: "countrycode", Value: "ROM"}, {Column: "language", Value: "Serbo-Croatian"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 0.1}},
				{{Column: "countrycode", Value: "DNK"}, {Column: "language", Value: "Norwegian"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 0.30000001}},
				{{Column: "countrycode", Value: "NGA"}, {Column: "language", Value: "Ijo"}, {Column: "isofficial", Value: false}, {Column: "percentage", Value: 1.8}},
			}},
		},
	},
	"aa8y/postgres-dataset:dellstore": {
		DBName: "dellstore",
		Tables: []serv_entities.SQLTable{
			{
				TableName: "public.categories",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "category", Type: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('categories_category_seq'::regclass)"), Position: 1},
					{Name: "categoryname", Type: "character varying(50)", IsNullable: false, Position: 2},
				},
				Constraints: []serv_entities.SQLTableConstraint{
					{Type: serv_entities.PrimaryKey, Name: "categories_pkey", Columns: []string{"category"}},
				},
			}, {
				TableName: "public.cust_hist",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "customerid", Type: "integer", IsNullable: false, Position: 1},
					{Name: "orderid", Type: "integer", IsNullable: false, Position: 2},
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 3},
				},
				ForeignKeys: []serv_entities.SQLTableForeignKey{
					{Name: "fk_cust_hist_customerid", Columns: []string{"customerid"}, ReferencedTable: "public.customers", ReferencedColumns: []string{"customerid"}, UpdateAction: serv_entities.NoAction, DeleteAction: serv_entities.Cascade},
				},
				Indexes: []serv_entities.SQLTableIndex{
					{Name: "ix_cust_hist_customerid", Type: "btree", Columns: []string{"customerid"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.customers",
				Columns: []serv_entities.SQLTableColumn{
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
				Constraints: []serv_entities.SQLTableConstraint{
					{Type: serv_entities.PrimaryKey, Name: "customers_pkey", Columns: []string{"customerid"}},
				},
				Indexes: []serv_entities.SQLTableIndex{
					{Name: "ix_cust_username", Type: "btree", Columns: []string{"username"}, Options: map[string]interface{}{"isUnique": true}},
				},
			}, {
				TableName: "public.inventory",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "quan_in_stock", Type: "integer", IsNullable: false, Position: 2},
					{Name: "sales", Type: "integer", IsNullable: false, Position: 3},
				},
				Constraints: []serv_entities.SQLTableConstraint{
					{Type: serv_entities.PrimaryKey, Name: "inventory_pkey", Columns: []string{"prod_id"}},
				},
			}, {
				TableName: "public.orderlines",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "orderlineid", Type: "integer", IsNullable: false, Position: 1},
					{Name: "orderid", Type: "integer", IsNullable: false, Position: 2},
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 3},
					{Name: "quantity", Type: "smallint", IsNullable: false, Position: 4},
					{Name: "orderdate", Type: "date", IsNullable: false, Position: 5},
				},
				ForeignKeys: []serv_entities.SQLTableForeignKey{
					{Name: "fk_orderid", Columns: []string{"orderid"}, ReferencedTable: "public.orders", ReferencedColumns: []string{"orderid"}, UpdateAction: serv_entities.NoAction, DeleteAction: serv_entities.Cascade},
				},
				Indexes: []serv_entities.SQLTableIndex{
					{Name: "ix_orderlines_orderid", Type: "btree", Columns: []string{"orderid", "orderlineid"}, Options: map[string]interface{}{"isUnique": true}},
				},
			}, {
				TableName: "public.orders",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "orderid", Type: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('orders_orderid_seq'::regclass)"), Position: 1},
					{Name: "orderdate", Type: "date", IsNullable: false, Position: 2},
					{Name: "customerid", Type: "integer", IsNullable: true, Position: 3},
					{Name: "netamount", Type: "numeric(12,2)", IsNullable: false, Position: 4},
					{Name: "tax", Type: "numeric(12,2)", IsNullable: false, Position: 5},
					{Name: "totalamount", Type: "numeric(12,2)", IsNullable: false, Position: 6},
				},
				Constraints: []serv_entities.SQLTableConstraint{
					{Type: serv_entities.PrimaryKey, Name: "orders_pkey", Columns: []string{"orderid"}},
				},
				ForeignKeys: []serv_entities.SQLTableForeignKey{
					{Name: "fk_customerid", Columns: []string{"customerid"}, ReferencedTable: "public.customers", ReferencedColumns: []string{"customerid"}, UpdateAction: serv_entities.NoAction, DeleteAction: serv_entities.SetNull},
				},
				Indexes: []serv_entities.SQLTableIndex{
					{Name: "ix_order_custid", Type: "btree", Columns: []string{"customerid"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.products",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, DefaultValue: helpers.Pointer("nextval('products_prod_id_seq'::regclass)"), Position: 1},
					{Name: "category", Type: "integer", IsNullable: false, Position: 2},
					{Name: "title", Type: "character varying(50)", IsNullable: false, Position: 3},
					{Name: "actor", Type: "character varying(50)", IsNullable: false, Position: 4},
					{Name: "price", Type: "numeric(12,2)", IsNullable: false, Position: 5},
					{Name: "special", Type: "smallint", IsNullable: true, Position: 6},
					{Name: "common_prod_id", Type: "integer", IsNullable: false, Position: 7},
				},
				Constraints: []serv_entities.SQLTableConstraint{
					{Type: serv_entities.PrimaryKey, Name: "products_pkey", Columns: []string{"prod_id"}},
				},
				Indexes: []serv_entities.SQLTableIndex{
					{Name: "ix_prod_category", Type: "btree", Columns: []string{"category"}, Options: map[string]interface{}{"isUnique": false}},
					{Name: "ix_prod_special", Type: "btree", Columns: []string{"special"}, Options: map[string]interface{}{"isUnique": false}},
				},
			}, {
				TableName: "public.reorder",
				Columns: []serv_entities.SQLTableColumn{
					{Name: "prod_id", Type: "integer", IsNullable: false, Position: 1},
					{Name: "date_low", Type: "date", IsNullable: false, Position: 2},
					{Name: "quan_low", Type: "integer", IsNullable: false, Position: 3},
					{Name: "date_reordered", Type: "date", IsNullable: true, Position: 4},
					{Name: "quan_reordered", Type: "integer", IsNullable: true, Position: 5},
					{Name: "date_expected", Type: "date", IsNullable: true, Position: 6},
				},
			},
		},
		TableContent: map[string]DBSQLTableContent{
			"public.categories": {DataLength: 16, ChunkSize: 4, Rows: []serv_entities.TableRow{
				{{Column: "category", Value: int64(1)}, {Column: "categoryname", Value: "Action"}},
				{{Column: "category", Value: int64(5)}, {Column: "categoryname", Value: "Comedy"}},
				{{Column: "category", Value: int64(9)}, {Column: "categoryname", Value: "Foreign"}},
				{{Column: "category", Value: int64(13)}, {Column: "categoryname", Value: "New"}},
			}},
			"public.customers": {DataLength: 20000, ChunkSize: 2000, Rows: []serv_entities.TableRow{
				{{Column: "customerid", Value: int64(1)}, {Column: "firstname", Value: "VKUUXF"}, {Column: "lastname", Value: "ITHOMQJNYX"}, {Column: "address1", Value: "4608499546 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "QSDPAGD"}, {Column: "state", Value: "SD"}, {Column: "zip", Value: int64(24101)}, {Column: "country", Value: "US"}, {Column: "region", Value: int64(1)}, {Column: "email", Value: "ITHOMQJNYX@dell.com"}, {Column: "phone", Value: "4608499546"}, {Column: "creditcardtype", Value: int64(1)}, {Column: "creditcard", Value: "1979279217775911"}, {Column: "creditcardexpiration", Value: "2012/03"}, {Column: "username", Value: "user1"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(55)}, {Column: "income", Value: int64(100000)}, {Column: "gender", Value: "M"}},
				{{Column: "customerid", Value: int64(2001)}, {Column: "firstname", Value: "LTMAHF"}, {Column: "lastname", Value: "LNNBYHHIGY"}, {Column: "address1", Value: "1001614924 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "XTLGXRX"}, {Column: "state", Value: "MD"}, {Column: "zip", Value: int64(60295)}, {Column: "country", Value: "US"}, {Column: "region", Value: int64(1)}, {Column: "email", Value: "LNNBYHHIGY@dell.com"}, {Column: "phone", Value: "1001614924"}, {Column: "creditcardtype", Value: int64(2)}, {Column: "creditcard", Value: "4158183055929840"}, {Column: "creditcardexpiration", Value: "2011/09"}, {Column: "username", Value: "user2001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(74)}, {Column: "income", Value: int64(20000)}, {Column: "gender", Value: "F"}},
				{{Column: "customerid", Value: int64(4001)}, {Column: "firstname", Value: "IMYAYU"}, {Column: "lastname", Value: "YTJXGCFHWJ"}, {Column: "address1", Value: "4942974925 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "EVYIIYY"}, {Column: "state", Value: "LA"}, {Column: "zip", Value: int64(87912)}, {Column: "country", Value: "US"}, {Column: "region", Value: int64(1)}, {Column: "email", Value: "YTJXGCFHWJ@dell.com"}, {Column: "phone", Value: "4942974925"}, {Column: "creditcardtype", Value: int64(2)}, {Column: "creditcard", Value: "3971776042133570"}, {Column: "creditcardexpiration", Value: "2010/01"}, {Column: "username", Value: "user4001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(22)}, {Column: "income", Value: int64(100000)}, {Column: "gender", Value: "M"}},
				{{Column: "customerid", Value: int64(6001)}, {Column: "firstname", Value: "RZKRNQ"}, {Column: "lastname", Value: "ZRDXBZHLZM"}, {Column: "address1", Value: "4214268075 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "KBTHSEF"}, {Column: "state", Value: "ND"}, {Column: "zip", Value: int64(23997)}, {Column: "country", Value: "US"}, {Column: "region", Value: int64(1)}, {Column: "email", Value: "ZRDXBZHLZM@dell.com"}, {Column: "phone", Value: "4214268075"}, {Column: "creditcardtype", Value: int64(2)}, {Column: "creditcard", Value: "7078812733292580"}, {Column: "creditcardexpiration", Value: "2010/11"}, {Column: "username", Value: "user6001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(70)}, {Column: "income", Value: int64(40000)}, {Column: "gender", Value: "M"}},
				{{Column: "customerid", Value: int64(8001)}, {Column: "firstname", Value: "RTYKLZ"}, {Column: "lastname", Value: "AWHGWKNKMF"}, {Column: "address1", Value: "9094764851 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "LAZWJQW"}, {Column: "state", Value: "WY"}, {Column: "zip", Value: int64(34834)}, {Column: "country", Value: "US"}, {Column: "region", Value: int64(1)}, {Column: "email", Value: "AWHGWKNKMF@dell.com"}, {Column: "phone", Value: "9094764851"}, {Column: "creditcardtype", Value: int64(5)}, {Column: "creditcard", Value: "5071015951689333"}, {Column: "creditcardexpiration", Value: "2008/04"}, {Column: "username", Value: "user8001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(38)}, {Column: "income", Value: int64(80000)}, {Column: "gender", Value: "F"}},
				{{Column: "customerid", Value: int64(10001)}, {Column: "firstname", Value: "QQGWSL"}, {Column: "lastname", Value: "NNTHRFAVRX"}, {Column: "address1", Value: "6739733410 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "KQLKYDL"}, {Column: "state", Value: nil}, {Column: "zip", Value: int64(0)}, {Column: "country", Value: "UK"}, {Column: "region", Value: int64(2)}, {Column: "email", Value: "NNTHRFAVRX@dell.com"}, {Column: "phone", Value: "6739733410"}, {Column: "creditcardtype", Value: int64(5)}, {Column: "creditcard", Value: "3180290846154094"}, {Column: "creditcardexpiration", Value: "2010/06"}, {Column: "username", Value: "user10001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(62)}, {Column: "income", Value: int64(100000)}, {Column: "gender", Value: "F"}},
				{{Column: "customerid", Value: int64(12001)}, {Column: "firstname", Value: "KKPQBY"}, {Column: "lastname", Value: "JDLUPSWWSD"}, {Column: "address1", Value: "7087720097 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "FMJQJVB"}, {Column: "state", Value: nil}, {Column: "zip", Value: int64(0)}, {Column: "country", Value: "Chile"}, {Column: "region", Value: int64(2)}, {Column: "email", Value: "JDLUPSWWSD@dell.com"}, {Column: "phone", Value: "7087720097"}, {Column: "creditcardtype", Value: int64(5)}, {Column: "creditcard", Value: "6277906919363887"}, {Column: "creditcardexpiration", Value: "2011/11"}, {Column: "username", Value: "user12001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(63)}, {Column: "income", Value: int64(20000)}, {Column: "gender", Value: "F"}},
				{{Column: "customerid", Value: int64(14001)}, {Column: "firstname", Value: "BPGIXX"}, {Column: "lastname", Value: "XWRAGPSDPR"}, {Column: "address1", Value: "6341127964 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "HCVCEGB"}, {Column: "state", Value: nil}, {Column: "zip", Value: int64(0)}, {Column: "country", Value: "Germany"}, {Column: "region", Value: int64(2)}, {Column: "email", Value: "XWRAGPSDPR@dell.com"}, {Column: "phone", Value: "6341127964"}, {Column: "creditcardtype", Value: int64(5)}, {Column: "creditcard", Value: "2779878668952733"}, {Column: "creditcardexpiration", Value: "2012/05"}, {Column: "username", Value: "user14001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(41)}, {Column: "income", Value: int64(80000)}, {Column: "gender", Value: "F"}},
				{{Column: "customerid", Value: int64(16001)}, {Column: "firstname", Value: "NLNNKJ"}, {Column: "lastname", Value: "OFPQTZQNZG"}, {Column: "address1", Value: "9248454082 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "UXEKOQN"}, {Column: "state", Value: nil}, {Column: "zip", Value: int64(0)}, {Column: "country", Value: "Australia"}, {Column: "region", Value: int64(2)}, {Column: "email", Value: "OFPQTZQNZG@dell.com"}, {Column: "phone", Value: "9248454082"}, {Column: "creditcardtype", Value: int64(3)}, {Column: "creditcard", Value: "3312776351813227"}, {Column: "creditcardexpiration", Value: "2009/08"}, {Column: "username", Value: "user16001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(75)}, {Column: "income", Value: int64(40000)}, {Column: "gender", Value: "F"}},
				{{Column: "customerid", Value: int64(18001)}, {Column: "firstname", Value: "XWYOVV"}, {Column: "lastname", Value: "OFBFKFIRLM"}, {Column: "address1", Value: "2933766882 Dell Way"}, {Column: "address2", Value: nil}, {Column: "city", Value: "ONXARPP"}, {Column: "state", Value: nil}, {Column: "zip", Value: int64(0)}, {Column: "country", Value: "Canada"}, {Column: "region", Value: int64(2)}, {Column: "email", Value: "OFBFKFIRLM@dell.com"}, {Column: "phone", Value: "2933766882"}, {Column: "creditcardtype", Value: int64(3)}, {Column: "creditcard", Value: "1694544910918921"}, {Column: "creditcardexpiration", Value: "2011/07"}, {Column: "username", Value: "user18001"}, {Column: "password", Value: "password"}, {Column: "age", Value: int64(67)}, {Column: "income", Value: int64(20000)}, {Column: "gender", Value: "F"}},
			}},
			"public.cust_hist": {DataLength: 60350, ChunkSize: 10000, Rows: []serv_entities.TableRow{
				{{Column: "customerid", Value: int64(7888)}, {Column: "orderid", Value: int64(1)}, {Column: "prod_id", Value: int64(9117)}},
				{{Column: "customerid", Value: int64(12033)}, {Column: "orderid", Value: int64(1985)}, {Column: "prod_id", Value: int64(3844)}},
				{{Column: "customerid", Value: int64(13953)}, {Column: "orderid", Value: int64(3958)}, {Column: "prod_id", Value: int64(7382)}},
				{{Column: "customerid", Value: int64(7638)}, {Column: "orderid", Value: int64(5946)}, {Column: "prod_id", Value: int64(6664)}},
				{{Column: "customerid", Value: int64(9534)}, {Column: "orderid", Value: int64(7947)}, {Column: "prod_id", Value: int64(7549)}},
				{{Column: "customerid", Value: int64(760)}, {Column: "orderid", Value: int64(9925)}, {Column: "prod_id", Value: int64(1585)}},
				{{Column: "customerid", Value: int64(11438)}, {Column: "orderid", Value: int64(11927)}, {Column: "prod_id", Value: int64(948)}},
			}},
			"public.inventory": {DataLength: 10000, ChunkSize: 2000, Rows: []serv_entities.TableRow{
				{{Column: "prod_id", Value: int64(1)}, {Column: "quan_in_stock", Value: int64(138)}, {Column: "sales", Value: int64(9)}},
				{{Column: "prod_id", Value: int64(2001)}, {Column: "quan_in_stock", Value: int64(235)}, {Column: "sales", Value: int64(6)}},
				{{Column: "prod_id", Value: int64(4001)}, {Column: "quan_in_stock", Value: int64(163)}, {Column: "sales", Value: int64(15)}},
				{{Column: "prod_id", Value: int64(6001)}, {Column: "quan_in_stock", Value: int64(218)}, {Column: "sales", Value: int64(17)}},
				{{Column: "prod_id", Value: int64(8001)}, {Column: "quan_in_stock", Value: int64(152)}, {Column: "sales", Value: int64(2)}},
			}},
			"public.orderlines": {DataLength: 60350, ChunkSize: 10000, Rows: []serv_entities.TableRow{
				{{Column: "orderlineid", Value: int64(1)}, {Column: "orderid", Value: int64(1)}, {Column: "prod_id", Value: int64(9117)}, {Column: "quantity", Value: int64(1)}, {Column: "orderdate", Value: time.Date(2004, 1, 27, 0, 0, 0, 0, time.UTC)}},
				{{Column: "orderlineid", Value: int64(2)}, {Column: "orderid", Value: int64(1985)}, {Column: "prod_id", Value: int64(3844)}, {Column: "quantity", Value: int64(3)}, {Column: "orderdate", Value: time.Date(2004, 2, 28, 0, 0, 0, 0, time.UTC)}},
				{{Column: "orderlineid", Value: int64(4)}, {Column: "orderid", Value: int64(3958)}, {Column: "prod_id", Value: int64(7382)}, {Column: "quantity", Value: int64(3)}, {Column: "orderdate", Value: time.Date(2004, 4, 15, 0, 0, 0, 0, time.UTC)}},
				{{Column: "orderlineid", Value: int64(6)}, {Column: "orderid", Value: int64(5946)}, {Column: "prod_id", Value: int64(6664)}, {Column: "quantity", Value: int64(2)}, {Column: "orderdate", Value: time.Date(2004, 6, 18, 0, 0, 0, 0, time.UTC)}},
				{{Column: "orderlineid", Value: int64(2)}, {Column: "orderid", Value: int64(7947)}, {Column: "prod_id", Value: int64(7549)}, {Column: "quantity", Value: int64(3)}, {Column: "orderdate", Value: time.Date(2004, 8, 6, 0, 0, 0, 0, time.UTC)}},
				{{Column: "orderlineid", Value: int64(1)}, {Column: "orderid", Value: int64(9925)}, {Column: "prod_id", Value: int64(1585)}, {Column: "quantity", Value: int64(1)}, {Column: "orderdate", Value: time.Date(2004, 10, 31, 0, 0, 0, 0, time.UTC)}},
				{{Column: "orderlineid", Value: int64(7)}, {Column: "orderid", Value: int64(11927)}, {Column: "prod_id", Value: int64(948)}, {Column: "quantity", Value: int64(3)}, {Column: "orderdate", Value: time.Date(2004, 12, 25, 0, 0, 0, 0, time.UTC)}},
			}},
			"public.orders": {DataLength: 12000, ChunkSize: 4000, Rows: []serv_entities.TableRow{
				{{Column: "orderid", Value: int64(1)}, {Column: "orderdate", Value: time.Date(2004, 1, 27, 0, 0, 0, 0, time.UTC)}, {Column: "customerid", Value: int64(7888)}, {Column: "netamount", Value: float64(313.24)}, {Column: "tax", Value: float64(25.84)}, {Column: "totalamount", Value: float64(339.08)}},
				{{Column: "orderid", Value: int64(4001)}, {Column: "orderdate", Value: time.Date(2004, 5, 12, 0, 0, 0, 0, time.UTC)}, {Column: "customerid", Value: int64(14005)}, {Column: "netamount", Value: float64(350.6)}, {Column: "tax", Value: float64(28.92)}, {Column: "totalamount", Value: float64(379.52)}},
				{{Column: "orderid", Value: int64(8001)}, {Column: "orderdate", Value: time.Date(2004, 9, 12, 0, 0, 0, 0, time.UTC)}, {Column: "customerid", Value: int64(9922)}, {Column: "netamount", Value: float64(384.52)}, {Column: "tax", Value: float64(31.72)}, {Column: "totalamount", Value: float64(416.24)}},
			}},
			"public.products": {DataLength: 10000, ChunkSize: 2000, Rows: []serv_entities.TableRow{
				{{Column: "prod_id", Value: int64(1)}, {Column: "category", Value: int64(14)}, {Column: "title", Value: "ACADEMY ACADEMY"}, {Column: "actor", Value: "PENELOPE GUINESS"}, {Column: "price", Value: float64(25.99)}, {Column: "special", Value: int64(0)}, {Column: "common_prod_id", Value: int64(1976)}},
				{{Column: "prod_id", Value: int64(2001)}, {Column: "category", Value: int64(5)}, {Column: "title", Value: "ADAPTATION ACADEMY"}, {Column: "actor", Value: "CARY NEESON"}, {Column: "price", Value: float64(19.99)}, {Column: "special", Value: int64(0)}, {Column: "common_prod_id", Value: int64(3385)}},
				{{Column: "prod_id", Value: int64(4001)}, {Column: "category", Value: int64(11)}, {Column: "title", Value: "AFRICAN ACADEMY"}, {Column: "actor", Value: "INGRID DOUGLAS"}, {Column: "price", Value: float64(20.99)}, {Column: "special", Value: int64(0)}, {Column: "common_prod_id", Value: int64(9099)}},
				{{Column: "prod_id", Value: int64(6001)}, {Column: "category", Value: int64(1)}, {Column: "title", Value: "AIRPLANE ACADEMY"}, {Column: "actor", Value: "TIM BRIDGES"}, {Column: "price", Value: float64(22.99)}, {Column: "special", Value: int64(0)}, {Column: "common_prod_id", Value: int64(5905)}},
				{{Column: "prod_id", Value: int64(8001)}, {Column: "category", Value: int64(13)}, {Column: "title", Value: "ALABAMA ACADEMY"}, {Column: "actor", Value: "ANNETTE KINNEAR"}, {Column: "price", Value: float64(11.99)}, {Column: "special", Value: int64(0)}, {Column: "common_prod_id", Value: int64(2439)}},
			}},
			"publiuc.reorder": {ChunkSize: 1},
		},
	},
}

func runPSQLReaderTests(t *testing.T) {
	for image, data := range imageData {
		db, cleanup := setupPostgreSQLContainer(t, image, data.DBName)

		dbReader := database_impl.NewPSQLDatabaseReader(db)
		testListSchemaNames(t, dbReader, data)
		testGetSchemaDefinition(t, dbReader, data)
		testGetSchemaDataLength(t, dbReader, data)
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

		table := schema.(*serv_entities.SQLTable)

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

func testGetSchemaDataLength(t *testing.T, dbReader services.DatabaseReader, expectedData DBSQLTestContent) {
	for _, table := range expectedData.Tables {
		dataLength, err := dbReader.GetSchemaDataLength(table.TableName)
		assert.Nil(t, err)
		assert.Equal(t, expectedData.TableContent[table.TableName].DataLength, dataLength)
	}
}

func testGetSchemaDataBatch(t *testing.T, dbReader services.DatabaseReader, expectedData DBSQLTestContent) {
	schemas := make([]entities.Schema, 0, len(expectedData.Tables))
	for _, table := range expectedData.Tables {
		schema, err := dbReader.GetSchemaDefinition(table.TableName)
		assert.Nil(t, err)
		assert.NotNil(t, schema)
		schemas = append(schemas, schema)
	}

	for _, schema := range schemas {
		table := schema.(*serv_entities.SQLTable)

		expectedRows := expectedData.TableContent[table.TableName].Rows
		var cursor entities.ChunkCursor = nil
		expectedRowIndex := 0
		for {
			rows, nextCursor, err := dbReader.GetSchemaDataChunk(table, uint(expectedData.TableContent[table.TableName].ChunkSize), cursor)
			assert.Nil(t, err)

			if len(rows) == 0 {
				break
			} else {
				assert.LessOrEqual(t, len(rows), expectedData.TableContent[table.TableName].ChunkSize)
				for _, columnValue := range expectedRows[expectedRowIndex] {
					switch a := columnValue.Value.(type) {
					case time.Time:
						var rowColumnValue interface{}
						for _, column := range rows[0].(serv_entities.TableRow) {
							if column.Column == columnValue.Column {
								rowColumnValue = column.Value
							}
						}
						e, ok := rowColumnValue.(time.Time)
						assert.True(t, ok)
						assert.Equal(t, e.Unix(), a.Unix())
					default:
						var rowColumnValue interface{}
						for _, column := range rows[0].(serv_entities.TableRow) {
							if column.Column == columnValue.Column {
								rowColumnValue = column.Value
							}
						}
						assert.Equal(t, columnValue.Value, rowColumnValue)
					}
				}
				expectedRowIndex++
			}

			cursor = nextCursor
		}
	}
}
