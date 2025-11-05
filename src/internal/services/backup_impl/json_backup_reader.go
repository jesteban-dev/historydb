package backup_impl

import (
	"encoding/json"
	"fmt"
	"historydb/src/internal/entities"
	serv_entities "historydb/src/internal/services/entities"
	"os"
	"path/filepath"
)

type JSONBackupReader struct {
	basePath string
}

func NewJSONBackupReader(basePath string) *JSONBackupReader {
	return &JSONBackupReader{basePath}
}

func (reader *JSONBackupReader) ReadBackupMetadata() (entities.BackupMetadata, error) {
	metadata := entities.BackupMetadata{}

	metadataPath := filepath.Join(reader.basePath, "backup.json")
	byteContent, err := os.ReadFile(metadataPath)
	if err != nil {
		return metadata, err
	}

	err = json.Unmarshal(byteContent, &metadata)
	return metadata, err
}

func (reader *JSONBackupReader) ReadSchema(hash string) (entities.Schema, error) {
	var schema entities.Schema

	schemaPath := filepath.Join(reader.basePath, "schemas", fmt.Sprintf("%s.json", hash))
	byteContent, err := os.ReadFile(schemaPath)
	if err != nil {
		return schema, err
	}

	schemaType := serv_entities.JSONSchemaType{}
	if err := json.Unmarshal(byteContent, &schemaType); err != nil {
		return schema, err
	}

	if schemaType.PrevRef != nil {
		schema, err = reader.ReadSchema(*schemaType.PrevRef)
		if err != nil {
			return schema, err
		}

		schemaDiff, err := reader.readSchemaDiffByType(schemaType.SchemaType, byteContent)
		if err != nil {
			return schema, err
		}
		schema = schema.ApplyDiff(schemaDiff)

		return schema, err
	} else {
		return reader.readSchemaByType(schemaType.SchemaType, byteContent)
	}
}

func (reader *JSONBackupReader) readSchemaByType(schemaType serv_entities.SchemaType, content []byte) (entities.Schema, error) {
	switch schemaType {
	case serv_entities.Relational:
		sqlTable := serv_entities.SQLTable{}
		err := json.Unmarshal(content, &sqlTable)
		return &sqlTable, err
	default:
		return nil, fmt.Errorf("unsupported schema type")
	}
}

func (reader *JSONBackupReader) readSchemaDiffByType(schemaType serv_entities.SchemaType, content []byte) (entities.SchemaDiff, error) {
	switch schemaType {
	case serv_entities.Relational:
		sqlTableDiff := serv_entities.SQLTableDiff{}
		err := json.Unmarshal(content, &sqlTableDiff)
		return &sqlTableDiff, err
	default:
		return nil, fmt.Errorf("unsupported schema type")
	}
}
