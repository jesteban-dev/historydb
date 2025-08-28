package backup_impl

import (
	"encoding/json"
	"fmt"
	"historydb/src/internal/entities"
	"io/fs"
	"os"
	"path/filepath"
)

type JSONBackupWriter struct {
	BaseBackupWriter
}

func NewJSONBackupWriter(basePath string) *JSONBackupWriter {
	return &JSONBackupWriter{BaseBackupWriter: BaseBackupWriter{basePath}}
}

func (writer *JSONBackupWriter) WriteSchemaDependency(tempPath string, dependency entities.SchemaDependency) error {
	hash, err := dependency.Hash()
	if err != nil {
		return err
	}

	content, err := json.Marshal(dependency)
	if err != nil {
		return err
	}

	pathToFile := filepath.Join(writer.basePath, tempPath, "schemas", "dependencies", fmt.Sprintf("%s.json", hash))
	if err := os.MkdirAll(filepath.Dir(pathToFile), 0755); err != nil {
		return err
	}

	return os.WriteFile(pathToFile, content, 0644)
}

func (writer *JSONBackupWriter) WriteSchemaDependencyDiff(tempPath string, dependencyDiff entities.SchemaDependencyDiff) error {
	content, err := json.Marshal(dependencyDiff)
	if err != nil {
		return err
	}

	pathToFile := filepath.Join(writer.basePath, tempPath, "schemas", "dependencies", fmt.Sprintf("%s.json", dependencyDiff.GetDependencyHash()))
	if err := os.MkdirAll(filepath.Dir(pathToFile), 0755); err != nil {
		return err
	}

	return os.WriteFile(pathToFile, content, 0644)
}

func (writer *JSONBackupWriter) WriteSchema(tempPath string, schema entities.Schema) error {
	hash, err := schema.Hash()
	if err != nil {
		return err
	}

	content, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	pathToFile := filepath.Join(writer.basePath, tempPath, "schemas", fmt.Sprintf("%s.json", hash))
	if err := os.MkdirAll(filepath.Dir(pathToFile), 0755); err != nil {
		return err
	}

	return os.WriteFile(pathToFile, content, 0644)
}

func (writer *JSONBackupWriter) WriteSchemaDiff(tempPath string, schemaDiff entities.SchemaDiff) error {
	content, err := json.Marshal(schemaDiff)
	if err != nil {
		return err
	}

	pathToFile := filepath.Join(writer.basePath, tempPath, "schemas", fmt.Sprintf("%s.json", schemaDiff.GetSchemaHash()))
	if err := os.MkdirAll(filepath.Dir(pathToFile), 0755); err != nil {
		return err
	}

	return os.WriteFile(pathToFile, content, 0644)
}

func (writer *JSONBackupWriter) CommitSnapshot(tempPath string, metadata entities.BackupMetadata) error {
	// Rename every file inside <uuid-snapshot> directory to root directory, as Rename is an atomic function
	tempDir := filepath.Join(writer.basePath, tempPath)
	if err := filepath.WalkDir(tempDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(tempDir, path)
		if err != nil {
			return err
		}

		dst := filepath.Join(writer.basePath, rel)
		err = os.Rename(path, dst)
		return err
	}); err != nil {
		return err
	}

	content, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	metadataTmpPath := filepath.Join(writer.basePath, "backup.json.tmp")
	metadataPath := filepath.Join(writer.basePath, "backup.json")

	// Write metadata into tmp file
	if err := os.WriteFile(metadataTmpPath, content, 0644); err != nil {
		return err
	}
	// Rename metadata tmp file to root metadata, as Rename is an atomic function
	if err := os.Rename(metadataTmpPath, metadataPath); err != nil {
		return fmt.Errorf("failed to rename metadata.json.tmp -> metadata.json: %w", err)
	}

	// Remove <uuid-snapshot> directory
	if err := os.RemoveAll(tempDir); err != nil {
		return fmt.Errorf("failed to remove tmp dir %s: %w", tempDir, err)
	}

	return nil
}

func (writer *JSONBackupWriter) RollbackSnapshot(tempPath string) error {
	tempDir := filepath.Join(writer.basePath, tempPath)
	return os.RemoveAll(tempDir)
}
