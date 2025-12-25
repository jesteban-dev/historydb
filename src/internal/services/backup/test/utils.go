package test

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type TestData struct {
	Name         string      `json:"name"`
	BackupPath   string      `json:"backupPath"`
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

func normalize(v interface{}) interface{} {
	switch t := v.(type) {
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	case string:
		if tm, err := time.Parse("2006-01-02 15:04:05.000", t); err == nil {
			return tm.UTC()
		}
		if tm, err := time.Parse("2006-01-02", t); err == nil {
			return tm.UTC()
		}
		return t
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, v := range t {
			out[k] = normalize(v)
		}
		return out
	case []interface{}:
		for i := range t {
			t[i] = normalize(t[i])
		}
		return t
	}
	return v
}
