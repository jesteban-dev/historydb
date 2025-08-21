package test

import (
	"fmt"
	"historydb/src/internal/services/entities"
	"log"
	"net/url"
	"strings"
	"testing"
)

type DBSQLTestContent struct {
	DBName string
	Tables []entities.SQLTable
}

var dbCases = []string{"postgres"}

func TestMain(t *testing.T) {
	for _, dbType := range dbCases {
		switch dbType {
		case "postgres":
			runPSQLReaderTests(t)
		default:
			log.Fatalf("Unsupported DB: %s", dbType)
		}
	}
}

func parseDbUrl(dbUrl string) (string, error) {
	dbUrl = strings.TrimSuffix(dbUrl, "?")

	u, err := url.Parse(dbUrl)
	if err != nil {
		return "", err
	}

	password, _ := u.User.Password()
	dbname := strings.TrimPrefix(u.Path, "/")

	q := u.Query()
	sslmode := q.Get("sslmode")
	if sslmode == "" {
		sslmode = "disable"
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", u.Hostname(), u.Port(), u.User.Username(), password, dbname, sslmode)
	return dsn, err
}
