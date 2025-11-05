package backup_impl

import (
	"fmt"
	"historydb/src/internal/services/entities"
	"os"
	"path/filepath"
)

// JsonBackupWriter represents the implementation of BackupWriter for JSON backup files.
type JSONBackupWriter struct {
	BaseBackupWriter
}

// NewJSONBackupWriter creates a new JsonBackupWriter with the provided base path.
//
// It returns a pointer to the created JSONBackupWriter.
func NewJSONBackupWriter(basePath string) *JSONBackupWriter {
	return &JSONBackupWriter{BaseBackupWriter: BaseBackupWriter{BasePath: basePath}}
}

// WriteSchemas implements the same function for BackupWriter interface that writes the schemas metadata for the backup.
//
// It returns an error if the process fails.
func (writer *JSONBackupWriter) WriteSchemas(schemas []entities.Schema) error {
	for _, schema := range schemas {
		hash, err := schema.Hash()
		if err != nil {
			return err
		}

		content, err := schema.ToJSON()
		if err != nil {
			return err
		}

		pathToFile := filepath.Join(writer.BasePath, "schemas", fmt.Sprintf("%x.json", hash))
		if err = os.WriteFile(pathToFile, content, 0644); err != nil {
			return err
		}
	}
	return nil
}
