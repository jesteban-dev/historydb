package test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
)

type TestData struct {
	Name         string      `json:"name"`
	Image        string      `json:"image"`
	InitScript   *string     `json:"initScript"`
	DBName       string      `json:"dbName"`
	ExpectedData interface{} `json:"expectedData"`
}

func extractJSONTestData(jsonPath string) ([]TestData, error) {
	file, err := os.Open(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("error opening test file: %v", err)
	}
	defer file.Close()

	var testData []TestData
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&testData); err != nil {
		return nil, fmt.Errorf("error decoding test file: %v", err)
	}

	return testData, nil
}

func parseDatabaseURL(dbUrl string) (string, error) {
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
