package backup_impl

import (
	"encoding/json"
	"fmt"
	"historydb/src/internal/entities"
	"os"
	"path/filepath"
)

type JSONBackupWriter struct {
	BaseBackupWriter
}

func NewJSONBackupWriter(basePath string) *JSONBackupWriter {
	return &JSONBackupWriter{BaseBackupWriter: BaseBackupWriter{basePath}}
}

func (writer *JSONBackupWriter) WriteSchema(schema entities.Schema) error {
	hash, err := schema.Hash()
	if err != nil {
		return err
	}

	content, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	pathToFile := filepath.Join(writer.basePath, "schemas", fmt.Sprintf("%s.json", hash))
	err = os.WriteFile(pathToFile, content, 0644)

	return err
}

func (writer *JSONBackupWriter) CommitSnapshotList(snapshotList []entities.Snapshot) error {
	content, err := json.Marshal(snapshotList)
	if err != nil {
		return err
	}

	metadataPath := filepath.Join(writer.basePath, "backup.json")
	err = os.WriteFile(metadataPath, content, 0644)
	return err
}
