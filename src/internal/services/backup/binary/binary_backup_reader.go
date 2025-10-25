package binary

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	"historydb/src/internal/services/entities/psql"
	"historydb/src/internal/services/entities/sql"
	"historydb/src/internal/utils/crypto"
	"historydb/src/internal/utils/decode"
	"io"
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

func (reader *BinaryBackupReader) CheckBackupExists() bool {
	_, err := os.Stat(filepath.Join(reader.backupPath, "metadata.hdb"))
	return err == nil
}

func (reader *BinaryBackupReader) GetBackupMetadata() (entities.BackupMetadata, error) {
	pathToFile := filepath.Join(reader.backupPath, "metadata.hdb")
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return entities.BackupMetadata{}, fmt.Errorf("%w: %s", services.ErrBackupDirNotExists, err.Error())
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

func (reader *BinaryBackupReader) GetSchemaDependency(dependencyRef string) (entities.SchemaDependency, bool, error) {
	pathToFile := filepath.Join(reader.backupPath, "schemas", "dependencies", fmt.Sprintf("%s.hdb", dependencyRef))
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return nil, false, err
	}

	if len(data) < sha256.Size {
		return nil, false, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	hashBytes := data[:sha256.Size]
	content := data[sha256.Size:]

	if ok := crypto.CheckDataSignature(hashBytes, content); !ok {
		return nil, false, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	if strings.HasPrefix(dependencyRef, "diffs") {
		prevRef, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, false, err
		}

		dependency, _, err := reader.GetSchemaDependency(*prevRef)
		if err != nil {
			return nil, false, err
		}

		dependencyDiff, err := reader.readSchemaDependencyDiffByType(dependency.GetDependencyType(), content)
		if err != nil {
			return nil, false, err
		}

		dependency = dependency.ApplyDiff(dependencyDiff)
		return dependency, true, nil
	} else {
		dependencyType, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, false, err
		}

		dependency, err := reader.readSchemaDependencyByType(entities.DependencyType(*dependencyType), content)
		return dependency, true, err
	}
}

func (reader *BinaryBackupReader) GetSchema(schemaRef string) (entities.Schema, bool, error) {
	pathToFile := filepath.Join(reader.backupPath, "schemas", fmt.Sprintf("%s.hdb", schemaRef))
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return nil, false, err
	}

	if len(data) < sha256.Size {
		return nil, false, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	hashBytes := data[:sha256.Size]
	content := data[sha256.Size:]

	if ok := crypto.CheckDataSignature(hashBytes, content); !ok {
		return nil, false, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	if strings.HasPrefix(schemaRef, "diffs") {
		prevRef, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, false, err
		}

		schema, _, err := reader.GetSchema(*prevRef)
		if err != nil {
			return nil, false, err
		}

		schemaDiff, err := reader.readSchemaDiffByType(schema.GetSchemaType(), content)
		if err != nil {
			return nil, false, err
		}

		schema = schema.ApplyDiff(schemaDiff)
		return schema, true, nil
	} else {
		schemaType, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, false, err
		}

		schema, err := reader.readSchemaByType(entities.SchemaType(*schemaType), content)
		return schema, false, err
	}
}

func (reader *BinaryBackupReader) GetSchemaRecordChunkRefsInBatch(batchRef string) ([]string, error) {
	pathToFile := filepath.Join(reader.backupPath, "data", fmt.Sprintf("%s.hdb", batchRef))
	f, err := os.Open(pathToFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Gets record type
	var recordTypeLength int64
	if err = binary.Read(f, binary.LittleEndian, &recordTypeLength); err != nil {
		return nil, err
	}
	recordType := make([]byte, recordTypeLength)
	if _, err := io.ReadFull(f, recordType); err != nil {
		return nil, err
	}

	if strings.HasPrefix(batchRef, "diffs") {
		// Gets prevBatchRef
		var prevBatchRefLength int64
		if err := binary.Read(f, binary.LittleEndian, &prevBatchRefLength); err != nil {
			return nil, err
		}
		prevBatchRef := make([]byte, prevBatchRefLength)
		if _, err := io.ReadFull(f, prevBatchRef); err != nil {
			return nil, err
		}

		chunkRefs, err := reader.GetSchemaRecordChunkRefsInBatch(string(prevBatchRef))
		if err != nil {
			return nil, err
		}

		return reader.readSchemaChunkRefsDiffByType(entities.RecordType(recordType), f, chunkRefs)
	} else {
		return reader.readSchemaChunkRefsByType(entities.RecordType(recordType), f)
	}
}

func (reader *BinaryBackupReader) GetSchemaRecordChunk(batchRef, chunkRef string) (entities.SchemaRecordChunk, bool, error) {
	pathToFile := filepath.Join(reader.backupPath, "data", fmt.Sprintf("%s.hdb", batchRef))
	f, err := os.Open(pathToFile)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	// Gets record type
	var recordTypeLength int64
	if err = binary.Read(f, binary.LittleEndian, &recordTypeLength); err != nil {
		return nil, false, err
	}
	recordType := make([]byte, recordTypeLength)
	if _, err := io.ReadFull(f, recordType); err != nil {
		return nil, false, err
	}

	if strings.HasPrefix(batchRef, "diffs") {
		// Gets prevBatchRef
		var prevBatchRefLength int64
		if err := binary.Read(f, binary.LittleEndian, &prevBatchRefLength); err != nil {
			return nil, false, err
		}
		prevBatchRef := make([]byte, prevBatchRefLength)
		if _, err := io.ReadFull(f, prevBatchRef); err != nil {
			return nil, false, err
		}

		diff, err := reader.readSchemaDataChunkDiffByType(entities.RecordType(recordType), chunkRef, f)
		if err != nil {
			return nil, false, err
		}

		if diff.GetPrevRef() != nil {
			chunk, _, err := reader.GetSchemaRecordChunk(string(prevBatchRef), *diff.GetPrevRef())
			if err != nil {
				return nil, false, err
			}

			chunk = chunk.ApplyDiff(diff)
			if chunk == nil {
				return nil, false, services.ErrBackupCorruptedFile
			}
			return chunk, true, nil
		}

		chunk := diff.ApplyDiffFromEmpty()
		if chunk == nil {
			return nil, false, services.ErrBackupCorruptedFile
		}
		return chunk, true, nil
	} else {
		chunk, err := reader.readSchemaRecordChunkByType(entities.RecordType(recordType), chunkRef, f)
		return chunk, false, err
	}
}

func (reader *BinaryBackupReader) GetRoutine(routineRef string) (entities.Routine, bool, error) {
	pathToFile := filepath.Join(reader.backupPath, "routines", fmt.Sprintf("%s.hdb", routineRef))
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return nil, false, err
	}

	if len(data) < sha256.Size {
		return nil, false, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	hashBytes := data[:sha256.Size]
	content := data[sha256.Size:]

	if ok := crypto.CheckDataSignature(hashBytes, content); !ok {
		return nil, false, fmt.Errorf("%w: %s", services.ErrBackupCorruptedFile, pathToFile)
	}

	if strings.HasPrefix(routineRef, "diffs") {
		prevRef, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, false, err
		}

		routine, _, err := reader.GetRoutine(*prevRef)
		if err != nil {
			return nil, false, err
		}

		routineDiff, err := reader.readRoutineDiffByType(routine.GetRoutineType(), content)
		if err != nil {
			return nil, false, err
		}

		routine = routine.ApplyDiff(routineDiff)
		return routine, true, nil
	} else {
		routineType, err := decode.DecodeString(bytes.NewBuffer(content))
		if err != nil {
			return nil, false, err
		}

		routine, err := reader.readRoutineByType(entities.RoutineType(*routineType), content)
		return routine, false, err
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

func (reader *BinaryBackupReader) readSchemaChunkRefsByType(recordType entities.RecordType, f *os.File) ([]string, error) {
	chunkRefs := []string{}

	switch recordType {
	case entities.SQLRecord:
		for {
			var chunkLength int64
			if err := binary.Read(f, binary.LittleEndian, &chunkLength); err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			} else if err != nil {
				return nil, err
			}
			chunkBytes := make([]byte, chunkLength)
			if _, err := io.ReadFull(f, chunkBytes); err != nil {
				return nil, err
			}

			chunkHash, err := decode.DecodeString(bytes.NewBuffer(chunkBytes))
			if err != nil {
				return nil, err
			}

			chunkRefs = append(chunkRefs, *chunkHash)
		}

		return chunkRefs, nil
	default:
		return nil, services.ErrRecordNotSupported
	}
}

func (reader *BinaryBackupReader) readSchemaChunkRefsDiffByType(recordType entities.RecordType, f *os.File, originalChunks []string) ([]string, error) {
	switch recordType {
	case entities.SQLRecord:
		for {
			var chunkLength int64
			if err := binary.Read(f, binary.LittleEndian, &chunkLength); err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			} else if err != nil {
				return nil, err
			}
			chunkBytes := make([]byte, chunkLength)
			if _, err := io.ReadFull(f, chunkBytes); err != nil {
				return nil, err
			}

			var diff sql.SQLRecordChunkDiff
			if err := diff.DecodeFromBytes(chunkBytes); err != nil {
				return nil, err
			}

			if diff.PrevRef == nil {
				originalChunks = append(originalChunks, *diff.Hash())
			} else {
				for i, v := range originalChunks {
					if v == *diff.PrevRef {
						originalChunks[i] = *diff.Hash()
					}
				}
			}
		}

		return originalChunks, nil
	default:
		return nil, services.ErrRecordNotSupported
	}
}

func (reader *BinaryBackupReader) readSchemaRecordChunkByType(recordType entities.RecordType, chunkRef string, f *os.File) (entities.SchemaRecordChunk, error) {
	switch recordType {
	case entities.SQLRecord:
		for {
			var chunkLength int64
			if err := binary.Read(f, binary.LittleEndian, &chunkLength); err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			} else if err != nil {
				return nil, err
			}
			chunkBytes := make([]byte, chunkLength)
			if _, err := io.ReadFull(f, chunkBytes); err != nil {
				return nil, err
			}

			chunkHash, err := decode.DecodeString(bytes.NewBuffer(chunkBytes))
			if err != nil {
				return nil, err
			}

			if crypto.CompareHashes(chunkRef, *chunkHash) {
				var chunk sql.SQLRecordChunk
				if err := chunk.DecodeFromBytes(chunkBytes); err != nil {
					return nil, err
				}
				return &chunk, nil
			}
		}

		return nil, services.ErrBackupChunkNotFound
	default:
		return nil, services.ErrRecordNotSupported
	}
}

func (reader *BinaryBackupReader) readSchemaDataChunkDiffByType(recordType entities.RecordType, chunkRef string, f *os.File) (entities.SchemaRecordChunkDiff, error) {
	switch recordType {
	case entities.SQLRecord:
		for {
			var chunkLength int64
			if err := binary.Read(f, binary.LittleEndian, &chunkLength); err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			} else if err != nil {
				return nil, err
			}
			chunkBytes := make([]byte, chunkLength)
			if _, err := io.ReadFull(f, chunkBytes); err != nil {
				return nil, err
			}

			var diff sql.SQLRecordChunkDiff
			if err := diff.DecodeFromBytes(chunkBytes); err != nil {
				return nil, err
			}

			if crypto.CompareHashes(chunkRef, *diff.Hash()) {
				return &diff, nil
			}
		}

		return nil, services.ErrBackupChunkNotFound
	default:
		return nil, services.ErrRecordNotSupported
	}
}

func (reader *BinaryBackupReader) readRoutineByType(routineType entities.RoutineType, content []byte) (entities.Routine, error) {
	switch routineType {
	case entities.PSQLFunction:
		var function psql.PSQLFunction
		if err := function.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &function, nil
	case entities.PSQLProcedure:
		var procedure psql.PSQLProcedure
		if err := procedure.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &procedure, nil
	case entities.PSQLTrigger:
		var trigger psql.PSQLTrigger
		if err := trigger.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &trigger, nil
	default:
		return nil, services.ErrRoutineNotSupported
	}
}

func (reader *BinaryBackupReader) readRoutineDiffByType(routineType entities.RoutineType, content []byte) (entities.RoutineDiff, error) {
	switch routineType {
	case entities.PSQLFunction:
		var diff psql.PSQLFunctionDiff
		if err := diff.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &diff, nil
	case entities.PSQLProcedure:
		var diff psql.PSQLProcedureDiff
		if err := diff.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &diff, nil
	case entities.PSQLTrigger:
		var diff psql.PSQLTriggerDiff
		if err := diff.DecodeFromBytes(content); err != nil {
			return nil, err
		}
		return &diff, nil
	default:
		return nil, services.ErrRoutineNotSupported
	}
}
