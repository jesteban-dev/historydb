package backup_impl

import (
	"encoding/json"
	"historydb/src/internal/entities"
	serv_entities "historydb/src/internal/services/entities"
	"os"
	"path/filepath"
)

type JSONBackupWriter struct {
	BaseBackupWriter
}

func NewJSONBackupWriter(basePath string) *JSONBackupWriter {
	writer := &JSONBackupWriter{}
	writer.BaseBackupWriter = BaseBackupWriter{basePath: basePath, self: writer}
	return writer
}

func (writer *JSONBackupWriter) CreateBackupFile() error {
	snapshotList := []serv_entities.JSONSnapshotInfo{}
	content, err := json.Marshal(snapshotList)
	if err != nil {
		return err
	}

	metadataPath := filepath.Join(writer.basePath, "backup.json")
	err = os.WriteFile(metadataPath, content, 0644)

	return err
}

func (writer *JSONBackupWriter) WriteSchema(schema entities.Schema) error {
	return nil
}
