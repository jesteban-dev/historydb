package backup_impl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/helpers"
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

func (reader *JSONBackupReader) ReadSchemaDependency(hash string) (entities.SchemaDependency, error) {
	var dependency entities.SchemaDependency

	dependencyPath := filepath.Join(reader.basePath, "schemas", "dependencies", fmt.Sprintf("%s.json", hash))
	byteContent, err := os.ReadFile(dependencyPath)
	if err != nil {
		return dependency, err
	}

	jsonData := serv_entities.JSONRefData{}
	if err := json.Unmarshal(byteContent, &jsonData); err != nil {
		return dependency, err
	}

	if jsonData.PrevRef != nil {
		sequence, err := reader.ReadSchemaDependency(*jsonData.PrevRef)
		if err != nil {
			return sequence, err
		}

		sequenceDiff := serv_entities.PSQLTableSequenceDiff{}
		if err := json.Unmarshal(byteContent, &sequenceDiff); err != nil {
			return sequence, err
		}
		sequence = sequence.ApplyDiff(sequenceDiff)

		return sequence, nil
	} else {
		sequence := serv_entities.PSQLTableSequence{}
		err := json.Unmarshal(byteContent, &sequence)
		return &sequence, err
	}
}

func (reader *JSONBackupReader) ReadSchema(hash string) (entities.Schema, error) {
	var schema entities.Schema

	schemaPath := filepath.Join(reader.basePath, "schemas", fmt.Sprintf("%s.json", hash))
	byteContent, err := os.ReadFile(schemaPath)
	if err != nil {
		return schema, err
	}

	jsonData := serv_entities.JSONSchemaData{}
	if err := json.Unmarshal(byteContent, &jsonData); err != nil {
		return schema, err
	}

	if jsonData.PrevRef != nil {
		schema, err = reader.ReadSchema(*jsonData.PrevRef)
		if err != nil {
			return schema, err
		}

		schemaDiff, err := reader.readSchemaDiffByType(jsonData.SchemaType, byteContent)
		if err != nil {
			return schema, err
		}
		schema = schema.ApplyDiff(schemaDiff)

		return schema, nil
	} else {
		return reader.readSchemaByType(jsonData.SchemaType, byteContent)
	}
}

func (reader *JSONBackupReader) ReadSchemaDataBatchChunks(hash string) ([]string, error) {
	dataPath := filepath.Join(reader.basePath, "data", fmt.Sprintf("%s.jsonl", hash))
	file, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var chunks []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var hash serv_entities.JSONDataChunkRef
		line := scanner.Bytes()

		if err := json.Unmarshal(line, &hash); err != nil {
			return nil, err
		}
		chunks = append(chunks, hash.Hash)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

func (reader *JSONBackupReader) ReadSchemaDataChunk(hash, chunkHash string) (entities.SchemaDataChunk, error) {
	dataPath := filepath.Join(reader.basePath, "data", fmt.Sprintf("%s.jsonl", hash))
	file, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var hash serv_entities.JSONDataChunkRef
		line := scanner.Bytes()

		if err := json.Unmarshal(line, &hash); err != nil {
			return nil, err
		}

		if helpers.CompareHashes(hash.Hash, chunkHash) {
			if hash.PrevRef != nil {
				chunk, err := reader.ReadSchemaDataChunk(hash.PrevRef.Batch, hash.PrevRef.Chunk)
				if err != nil {
					return chunk, err
				}

				chunkDiff, err := reader.readDataDiffByType(hash.SchemaType, line)
				if err != nil {
					return chunk, err
				}
				chunk = chunk.ApplyDiff(chunkDiff)

				return chunk, nil
			} else {
				return reader.readDataByType(hash.SchemaType, line)
			}
		}
	}

	return nil, fmt.Errorf("data chunk not found")
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

func (reader *JSONBackupReader) readDataByType(schemaType serv_entities.SchemaType, content []byte) (entities.SchemaDataChunk, error) {
	switch schemaType {
	case serv_entities.Relational:
		sqlChunk := serv_entities.TableRowChunk{}
		err := json.Unmarshal(content, &sqlChunk)
		return &sqlChunk, err
	default:
		return nil, fmt.Errorf("unsupported schema type")
	}
}

func (reader *JSONBackupReader) readDataDiffByType(schemaType serv_entities.SchemaType, content []byte) (entities.SchemaDataChunkDiff, error) {
	switch schemaType {
	case serv_entities.Relational:
		sqlChunkDiff := serv_entities.TableRowChunkDiff{}
		err := json.Unmarshal(content, &sqlChunkDiff)
		return &sqlChunkDiff, err
	default:
		return nil, fmt.Errorf("unsupported schema type")
	}
}
