package binary

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	"historydb/src/internal/services/entities/psql"
	"historydb/src/internal/services/entities/sql"
	"historydb/src/internal/utils/crypto"
	"historydb/src/internal/utils/decode"
	"os"
	"path/filepath"
	"strings"
)

type BinaryBackupReader struct {
	backupPath string
}

func NewBinaryBackupReader(backupPath string) *BinaryBackupReader {
	return &BinaryBackupReader{backupPath}
}

func (reader *BinaryBackupReader) GetBackupMetadata() (entities.BackupMetadata, error) {
	pathToFile := filepath.Join(reader.backupPath, "metadata.hdb")
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return entities.BackupMetadata{}, err
	}

	if len(data) < sha256.Size {
		return entities.BackupMetadata{}, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	hashBytes := data[:sha256.Size]
	content := data[sha256.Size:]

	if ok := crypto.CheckDataSignature(hashBytes, content); !ok {
		return entities.BackupMetadata{}, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	var metadata entities.BackupMetadata
	if err := metadata.DecodeFromBytes(content); err != nil {
		return entities.BackupMetadata{}, err
	}

	return metadata, nil
}

func (reader *BinaryBackupReader) GetBackupSnapshot(snapshotId string) (entities.BackupSnapshot, error) {
	pathToFile := filepath.Join(reader.backupPath, "snapshots", fmt.Sprintf("%s.hdb", snapshotId))
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return entities.BackupSnapshot{}, err
	}

	if len(data) < sha256.Size {
		return entities.BackupSnapshot{}, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	hashBytes := data[:sha256.Size]
	content := data[sha256.Size:]

	if ok := crypto.CheckDataSignature(hashBytes, content); !ok {
		return entities.BackupSnapshot{}, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	var snapshot entities.BackupSnapshot
	if err := snapshot.DecodeFromBytes(content); err != nil {
		return entities.BackupSnapshot{}, err
	}

	return snapshot, nil
}

func (reader *BinaryBackupReader) GetSchemaDependency(filename string) (entities.SchemaDependency, error) {
	pathToFile := filepath.Join(reader.backupPath, "schemas", "dependencies", fmt.Sprintf("%s.hdb", filename))
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return nil, err
	}

	if len(data) < sha256.Size {
		return nil, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	hashBytes := data[:sha256.Size]
	content := data[sha256.Size:]

	if ok := crypto.CheckDataSignature(hashBytes, content); !ok {
		return nil, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	if strings.HasPrefix(filename, "diffs") {
		prevRef, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, err
		}

		dependency, err := reader.GetSchemaDependency(*prevRef)
		if err != nil {
			return nil, err
		}

		dependencyDiff, err := reader.readSchemaDependencyDiffByType(dependency.GetDependencyType(), content)
		if err != nil {
			return nil, err
		}

		dependency = dependency.ApplyDiff(dependencyDiff)
		return dependency, nil
	} else {
		dependencyType, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, err
		}

		return reader.readSchemaDependencyByType(entities.DependencyType(*dependencyType), content)
	}
}

func (reader *BinaryBackupReader) GetSchema(filename string) (entities.Schema, error) {
	pathToFile := filepath.Join(reader.backupPath, "schemas", fmt.Sprintf("%s.hdb", filename))
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return nil, err
	}

	if len(data) < sha256.Size {
		return nil, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	hashBytes := data[:sha256.Size]
	content := data[sha256.Size:]

	if ok := crypto.CheckDataSignature(hashBytes, content); !ok {
		return nil, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	if strings.HasPrefix(filename, "diffs") {
		prevRef, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, err
		}

		schema, err := reader.GetSchema(*prevRef)
		if err != nil {
			return nil, err
		}

		schemaDiff, err := reader.readSchemaDiffByType(schema.GetSchemaType(), content)
		if err != nil {
			return nil, err
		}

		schema = schema.ApplyDiff(schemaDiff)
		return schema, nil
	} else {
		schemaType, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, err
		}

		return reader.readSchemaByType(entities.SchemaType(*schemaType), content)
	}
}

func (reader *BinaryBackupReader) readSchemaDependencyByType(dependencyType entities.DependencyType, content []byte) (entities.SchemaDependency, error) {
	switch dependencyType {
	case entities.PSQLSequence:
		var seq psql.PSQLSequence
		err := seq.DecodeFromBytes(content)
		if err != nil {
			return nil, err
		}
		return &seq, nil
	default:
		return nil, fmt.Errorf("unsupported schema dependency type")
	}
}

func (reader *BinaryBackupReader) readSchemaDependencyDiffByType(dependencyType entities.DependencyType, content []byte) (entities.SchemaDependencyDiff, error) {
	switch dependencyType {
	case entities.PSQLSequence:
		var diff psql.PSQLSequenceDiff
		if err := diff.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &diff, nil
	default:
		return nil, services.ErrDependencyNotSupported
	}
}

func (reader *BinaryBackupReader) readSchemaByType(schemaType entities.SchemaType, content []byte) (entities.Schema, error) {
	switch schemaType {
	case entities.SQLTable:
		var table sql.SQLTable
		if err := table.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &table, nil
	default:
		return nil, services.ErrSchemaNotSupported
	}
}

func (reader *BinaryBackupReader) readSchemaDiffByType(schemaType entities.SchemaType, content []byte) (entities.SchemaDiff, error) {
	switch schemaType {
	case entities.SQLTable:
		var diff sql.SQLTableDiff
		if err := diff.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &diff, nil
	default:
		return nil, services.ErrSchemaNotSupported
	}
}
