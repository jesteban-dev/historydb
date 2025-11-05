package usecases

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	backup_services "historydb/src/internal/services/backup"
	database_services "historydb/src/internal/services/database"
	"historydb/src/internal/usecases/dtos"
	"historydb/src/internal/utils/crypto"
	"math"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

type BackupUsecasesImpl struct {
	dbFactory     database_services.DatabaseFactory
	backupFactory backup_services.BackupFactory
	logger        *logrus.Logger
}

func NewBackupUsecasesImpl(dbFactory database_services.DatabaseFactory, backupFactory backup_services.BackupFactory, logger *logrus.Logger) *BackupUsecasesImpl {
	return &BackupUsecasesImpl{dbFactory, backupFactory, logger}
}

func (uc *BackupUsecasesImpl) GetBackupMetadata() *entities.BackupMetadata {
	backupReader := uc.backupFactory.CreateReader()

	backupMetadata, err := backupReader.GetBackupMetadata()
	if err != nil {
		if errors.Is(err, services.ErrBackupDirNotExists) {
			fmt.Println("The specified backup path does not contain any backup.")
		} else if errors.Is(err, services.ErrBackupCorruptedFile) {
			fmt.Println("The specified backup is corrupted.")
		}

		uc.logger.Errorf("could not retrieve backup metadata: %v\n", err)
		return nil
	}

	return &backupMetadata
}

func (uc *BackupUsecasesImpl) GetSnapshot(snapshotId string) *entities.BackupSnapshot {
	backupReader := uc.backupFactory.CreateReader()

	snapshot, err := backupReader.GetBackupSnapshot(snapshotId)
	if err != nil {
		if errors.Is(err, services.ErrBackupCorruptedFile) {
			fmt.Println("The specified snapshot is corrupted.")
		}

		uc.logger.Errorf("could not retrieve bacup snapshot: %v\n", err)
		return nil
	}

	return &snapshot
}

func (uc *BackupUsecasesImpl) CreateSnapshot(first bool) *entities.BackupSnapshot {
	backupWriter := uc.backupFactory.CreateWriter()

	snapshot := entities.BackupSnapshot{
		SnapshotId:         uuid.NewString(),
		Timestamp:          time.Now(),
		SchemaDependencies: make(map[string]string),
		Schemas:            make(map[string]string),
		Data:               make(map[string]entities.BackupSnapshotSchemaData),
	}
	if first {
		if err := backupWriter.CreateBackupStructure(); err != nil {
			if errors.Is(err, services.ErrBackupDirExists) {
				fmt.Println("The specified backup path already exists.\nTo create a new backup you need to provide a non-existing path, that will be created in the process.")
			}

			uc.logger.Errorf("could not create backup directory: %v\n", err)
			return nil
		}
	}

	if err := backupWriter.BeginSnapshot(&snapshot); err != nil {
		uc.logger.Errorf("could not begin snapshot: %v\n", err)
		return nil
	}

	return &snapshot
}

func (uc *BackupUsecasesImpl) CommitSnapshot(metadata *entities.BackupMetadata, snapshot *entities.BackupSnapshot) bool {
	backupWriter := uc.backupFactory.CreateWriter()

	if metadata == nil {
		metadata = &entities.BackupMetadata{DatabaseEngine: uc.dbFactory.GetDBEngine(), Snapshots: []entities.BackupMetadataSnapshot{{Timestamp: snapshot.Timestamp, SnapshotId: snapshot.SnapshotId}}}
	} else {
		metadata.Snapshots = append(metadata.Snapshots, entities.BackupMetadataSnapshot{Timestamp: snapshot.Timestamp, SnapshotId: snapshot.SnapshotId})
	}

	if err := backupWriter.CommitSnapshot(metadata); err != nil {
		uc.logger.Errorf("could not save snapshot: %v\n", err)
		return false
	}

	fmt.Println("Backup saved successfully!")
	return true
}

func (uc *BackupUsecasesImpl) RollbackSnapshot(first bool) {
	backupWriter := uc.backupFactory.CreateWriter()
	fmt.Println("Process failed. Aborting operation...")

	if first {
		fmt.Println("  - Cleaning backup directory...")
		if err := backupWriter.DeleteBackupStructure(); err != nil {
			uc.logger.Errorf("could not delete backup directory: %v\n", err)
			return
		}
		fmt.Println("  + Clean completed!")
	} else {
		fmt.Println("  - Rollback to previous state...")
		if err := backupWriter.RollbackSnapshot(); err != nil {
			uc.logger.Errorf("could not rollback to previous state: %v\n", err)
			return
		}
		fmt.Println("  + Rollback completed!")
	}
	fmt.Println("Closing app...")
}

func (uc *BackupUsecasesImpl) BackupSchemaDependencies(snapshot *entities.BackupSnapshot) bool {
	dbReader := uc.dbFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// List schema dependencies from database
	schemaDependencies, err := dbReader.ListSchemaDependencies()
	if err != nil {
		uc.logger.Errorf("could not list schema dependencies from DB: %v\n", err)
		return false
	}

	// Backups all schema dependencies
	schemaDependenciesProgress := progressbar.NewOptions(len(schemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema dependencies...", len(schemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, dependency := range schemaDependencies {
		// Saves dependency into backup
		if err := backupWriter.SaveSchemaDependency(dependency); err != nil {
			uc.logger.Errorf("could not save %s schema dependency into backup: %v\n", dependency.GetName(), err)
			return false
		}

		// Saves ref into snapshot
		snapshot.SchemaDependencies[dependency.GetName()] = dependency.Hash()
		schemaDependenciesProgress.Add(1)
	}

	fmt.Println("  - All schema dependencies saved successfully!")
	return true
}

func (uc *BackupUsecasesImpl) SnapshotSchemaDependencies(lastSnapshot, snapshot *entities.BackupSnapshot) bool {
	dbReader := uc.dbFactory.CreateReader()
	backupReader := uc.backupFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// List schema dependencies from database
	schemaDependencies, err := dbReader.ListSchemaDependencies()
	if err != nil {
		uc.logger.Errorf("could not list schema dependencies from DB: %v\n", err)
		return false
	}

	// Backups all schema dependencies
	schemaDependenciesProgress := progressbar.NewOptions(len(schemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Updating all %d schema dependencies...", len(schemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, dependency := range schemaDependencies {
		// If no condition it stays unmodified
		hash := dependency.Hash()
		prevHash, ok := lastSnapshot.SchemaDependencies[dependency.GetName()]
		if !ok {
			// New dependency to write into backup
			if err := backupWriter.SaveSchemaDependency(dependency); err != nil {
				uc.logger.Errorf("could not save %s schema dependency into backup: %v\n", dependency.GetName(), err)
				return false
			}

			snapshot.SchemaDependencies[dependency.GetName()] = hash
		} else if !crypto.CompareHashes(prevHash, hash) {
			// Updates dependency into backup
			prevDependency, isDiff, err := backupReader.GetSchemaDependency(prevHash)
			if err != nil {
				if errors.Is(err, services.ErrBackupCorruptedFile) {
					fmt.Printf("The %s schema dependency in backup is corrupted\n", dependency.GetName())
				}
				uc.logger.Errorf("could not read %s schema dependency from backup: %v\n", dependency.GetName(), err)
				return false
			}

			dependencyDiff := dependency.Diff(prevDependency, isDiff)
			if err := backupWriter.SaveSchemaDependencyDiff(dependencyDiff); err != nil {
				uc.logger.Errorf("could not update %s schema dependency into backup: %v\n", dependency.GetName(), err)
				return false
			}

			snapshot.SchemaDependencies[dependency.GetName()] = fmt.Sprintf("diffs/%s", hash)
		} else {
			snapshot.SchemaDependencies[dependency.GetName()] = prevHash
		}

		schemaDependenciesProgress.Add(1)
	}

	fmt.Println("  - All schema dependencies updated successfully!")
	return true
}

func (uc *BackupUsecasesImpl) BackupSchemas(snapshot *entities.BackupSnapshot) []entities.Schema {
	dbReader := uc.dbFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// Lists schemas from database
	schemaNames, err := dbReader.ListSchemaNames()
	if err != nil {
		uc.logger.Errorf("could not list schemas from DB: %v\n", err)
		return nil
	}

	// Backups all schema definitions
	schemas := make([]entities.Schema, 0, len(schemaNames))
	schemaProgress := progressbar.NewOptions(len(schemaNames), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema definitions...", len(schemaNames))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, schemaName := range schemaNames {
		// Reads schema definition
		schema, err := dbReader.GetSchemaDefinition(schemaName)
		if err != nil {
			uc.logger.Errorf("could not retrieve %s schema definition from DB: %v\n", schemaName, err)
			return nil
		}
		hash := schema.Hash()

		schemas = append(schemas, schema)

		// Saves schema into the backup
		if err := backupWriter.SaveSchema(schema); err != nil {
			uc.logger.Errorf("could not save %s schema definition into backup: %v\n", schemaName, err)
			return nil
		}

		// Saves the schema ref into the snapshot
		snapshot.Schemas[schemaName] = hash
		schemaProgress.Add(1)
	}

	fmt.Println("  - All schema definitions saved successfully")
	return schemas
}

func (uc *BackupUsecasesImpl) SnapshotSchemas(lastSnapshot, snapshot *entities.BackupSnapshot) []entities.Schema {
	dbReader := uc.dbFactory.CreateReader()
	backupReader := uc.backupFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// Lists schemas from database
	schemaNames, err := dbReader.ListSchemaNames()
	if err != nil {
		uc.logger.Errorf("could not list schemas from DB: %v\n", err)
		return nil
	}

	// Backups all schema definitions
	schemas := make([]entities.Schema, 0, len(schemaNames))
	schemaProgress := progressbar.NewOptions(len(schemaNames), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema definitions...", len(schemaNames))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, schemaName := range schemaNames {
		// Reads schema definition
		schema, err := dbReader.GetSchemaDefinition(schemaName)
		if err != nil {
			uc.logger.Errorf("could not retrieve %s schema definition from DB: %v\n", schemaName, err)
			return nil
		}
		hash := schema.Hash()

		// If no condition it stays unmodified
		prevHash, ok := lastSnapshot.Schemas[schemaName]
		if !ok {
			// New schema to write into backup
			if err := backupWriter.SaveSchema(schema); err != nil {
				uc.logger.Errorf("could not save %s schema definition into backup: %v\n", schemaName, err)
				return nil
			}

			snapshot.Schemas[schemaName] = hash
		} else if !crypto.CompareHashes(prevHash, hash) {
			// Updates schema into backup
			prevSchema, isDiff, err := backupReader.GetSchema(prevHash)
			if err != nil {
				if errors.Is(err, services.ErrBackupCorruptedFile) {
					fmt.Printf("The %s schema in backup is corrupted\n", schema.GetName())
				}

				uc.logger.Errorf("could not read %s schema definition from backup: %v\n", schemaName, err)
				return nil
			}

			schemaDiff := schema.Diff(prevSchema, isDiff)
			if err := backupWriter.SaveSchemaDiff(schemaDiff); err != nil {
				uc.logger.Errorf("could not update %s schema definition into backup: %v\n", schemaName, err)
				return nil
			}

			snapshot.Schemas[schemaName] = fmt.Sprintf("diffs/%s", hash)
		} else {
			snapshot.Schemas[schemaName] = prevHash
		}

		schemas = append(schemas, schema)
		schemaProgress.Add(1)
	}

	fmt.Println("  - All schema definitions updated successfully")
	return schemas
}

func (uc *BackupUsecasesImpl) BackupSchemaRecords(snapshot *entities.BackupSnapshot, schema entities.Schema) bool {
	dbReader := uc.dbFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// Calculates batch and chunk sizes
	recordMetadata, err := dbReader.GetSchemaRecordMetadata(schema.GetName())
	if err != nil {
		uc.logger.Errorf("could not retrieve %s schema record metadata: %v\n", schema.GetName(), err)
		return false
	}

	var batchSize, chunkSize int64
	if recordMetadata.MaxRecordSize < entities.LIMIT_RECORD_SIZE {
		batchSize = int64(math.Min(float64(entities.SMALL_FILE_MAX_SIZE)/float64(recordMetadata.MaxRecordSize), float64(entities.MAX_BATCH_LENGTH)))
		chunkSize = batchSize / 100
	} else {
		batchSize = int64(entities.BIG_FILE_MAX_SIZE / recordMetadata.MaxRecordSize)
		chunkSize = batchSize / 10
	}

	// Loops until savedRecords == Database total records
	savedRecords := 0
	batchHashes := []string{}
	dataProgress := progressbar.NewOptions(int(math.Ceil(float64(recordMetadata.Count)/float64(chunkSize))), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving %s schema records...", schema.GetName())), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for savedRecords < recordMetadata.Count {
		currentBatchSize := 0
		tempBatchName := uuid.NewString()
		batchHashBytes := sha256.New()

		// Loops until batch is full or there is no more content
		var cursor interface{}
		for currentBatchSize < int(batchSize) {
			// Reads record chunk from DB
			chunk, nextCursor, err := dbReader.GetSchemaRecordChunk(schema, chunkSize, cursor)
			if err != nil {
				uc.logger.Errorf("could not retrieve record chunk from %s schema: %v\n", schema.GetName(), err)
				return false
			}

			if chunk.Length() == 0 {
				break
			} else {
				currentBatchSize += chunk.Length()
			}

			// Saves chunks in backup temporal batch file
			chunkHash := chunk.Hash()
			if err := backupWriter.SaveSchemaRecordChunk(tempBatchName, chunk); err != nil {
				uc.logger.Errorf("could not save record chunk in backup: %v\n", err)
				return false
			}

			cursor = nextCursor
			batchHashBytes.Write([]byte(chunkHash))
			batchHashBytes.Write([]byte("|"))
			dataProgress.Add(1)
		}

		// After full batch is completed, renames the temp batch file to the final one
		batchHash := hex.EncodeToString(batchHashBytes.Sum(nil))
		if err := backupWriter.SaveSchemaRecordBatch(tempBatchName, batchHash); err != nil {
			uc.logger.Errorf("could not save record batch in backup: %v\n", err)
			return false
		}

		batchHashes = append(batchHashes, batchHash)
		savedRecords += currentBatchSize
	}
	fmt.Println("  - All schema records saved successfully")

	snapshot.Data[schema.GetName()] = entities.BackupSnapshotSchemaData{
		BatchSize: batchSize,
		ChunkSize: chunkSize,
		Data:      batchHashes,
	}
	return true
}

func (uc *BackupUsecasesImpl) SnapshotSchemaRecords(lastSnapshot, snapshot *entities.BackupSnapshot, schema entities.Schema) bool {
	dbReader := uc.dbFactory.CreateReader()
	backupReader := uc.backupFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	recordMetadata, err := dbReader.GetSchemaRecordMetadata(schema.GetName())
	if err != nil {
		uc.logger.Errorf("could not retrieve %s schema record metadata: %v\n", schema.GetName(), err)
		return false
	}

	backupMetadata, ok := lastSnapshot.Data[schema.GetName()]
	if !ok {
		uc.logger.Errorf("could not retrieve schema record metadata from previous backup snapshot for %s schema\n", schema.GetName())
		return false
	}

	savedRecords := 0
	batchIndex := 0
	snapshotData := entities.BackupSnapshotSchemaData{
		BatchSize: backupMetadata.BatchSize,
		ChunkSize: backupMetadata.ChunkSize,
		Data:      []string{},
	}
	// Loops until savedRecords == Database total records
	dataProgress := progressbar.NewOptions(int(math.Ceil(float64(recordMetadata.Count)/float64(backupMetadata.ChunkSize))), progressbar.OptionSetDescription(fmt.Sprintf("  + Updating %s schema records...", schema.GetName())), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for savedRecords < recordMetadata.Count {
		currentBatchSize := 0
		batchHashBytes := sha256.New()
		batchChunks := []dtos.BatchChunkInfo{}

		// Calculates all new chunk hashes in batch
		var cursor interface{}
		for currentBatchSize < int(backupMetadata.BatchSize) {
			// Reads record chunk from DB
			chunk, nextCursor, err := dbReader.GetSchemaRecordChunk(schema, backupMetadata.ChunkSize, cursor)
			if err != nil {
				uc.logger.Errorf("could not retrieve record chunk from %s schema: %v\n", schema.GetName(), err)
				return false
			}

			if chunk.Length() == 0 {
				break
			} else {
				currentBatchSize += chunk.Length()
			}

			// Saves chunk hash and cursor if then is needed
			chunkHash := chunk.Hash()
			batchChunks = append(batchChunks, dtos.BatchChunkInfo{Hash: chunkHash, Cursor: cursor})
			cursor = nextCursor
			batchHashBytes.Write([]byte(chunkHash))
			batchHashBytes.Write([]byte("|"))
			dataProgress.Add(1)
		}

		// Calculates new batch hash
		batchHash := hex.EncodeToString(batchHashBytes.Sum(nil))

		if len(backupMetadata.Data) > batchIndex {
			// Current batch is in backup -> Compare hashes, if it is equal we continue to the next batch
			oldBatchHash, _ := strings.CutPrefix(backupMetadata.Data[batchIndex], "diffs/")

			if !crypto.CompareHashes(oldBatchHash, batchHash) {
				// Retrieve backup chunks
				backupChunks, err := backupReader.GetSchemaRecordChunkRefsInBatch(backupMetadata.Data[batchIndex])
				if err != nil {
					uc.logger.Errorf("could not retrieve chunks from backup batch in %s schema: %v\n", schema.GetName(), err)
					return false
				}

				// Compare each chunk hash -> If they match, continue to next chunk
				for i := 0; i < int(math.Max(float64(len(backupChunks)), float64(len(batchChunks)))); i++ {
					var recordDiff entities.SchemaRecordChunkDiff

					if i >= len(batchChunks) {
						// Chunk does not exist in new batch -> Delete chunk from bakup batch
						backupChunk, isDiff, err := backupReader.GetSchemaRecordChunk(backupMetadata.Data[batchIndex], backupChunks[i])
						if err != nil {
							uc.logger.Errorf("could not retrieve record chunk from backup: %v\n", err)
							return false
						}

						recordDiff = backupChunk.DiffToEmpty(isDiff)
					} else {
						recordChunk, _, err := dbReader.GetSchemaRecordChunk(schema, backupMetadata.ChunkSize, batchChunks[i].Cursor)
						if err != nil {
							uc.logger.Errorf("could not retrieve record chunk from %s schema: %v\n", schema.GetName(), err)
							return false
						}

						if i < len(backupChunks) && i < len(batchChunks) && !crypto.CompareHashes(backupChunks[i], batchChunks[i].Hash) {
							// Chunk hashes does not match -> Replace chunk

							// Reads old chunk from backup
							backupChunk, isDiff, err := backupReader.GetSchemaRecordChunk(backupMetadata.Data[batchIndex], backupChunks[i])
							if err != nil {
								uc.logger.Errorf("could not retrieve record chunk from backup: %v\n", err)
								return false
							}

							recordDiff = recordChunk.Diff(backupChunk, isDiff)
						} else if i >= len(backupChunks) {
							// Chunk does not exist in backup -> Create new chunk in backup batch
							recordDiff = recordChunk.DiffFromEmpty()
						}
					}

					if err := backupWriter.SaveSchemaRecordChunkDiff(backupMetadata.Data[batchIndex], fmt.Sprintf("diffs/%s", batchHash), recordDiff); err != nil {
						uc.logger.Errorf("could not update %s schema record chunk into backup: %v\n", schema.GetName(), err)
						return false
					}
				}

				snapshotData.Data = append(snapshotData.Data, fmt.Sprintf("diffs/%s", batchHash))
			} else {
				snapshotData.Data = append(snapshotData.Data, backupMetadata.Data[batchIndex])
			}
		} else {
			// New batch to save in backup
			tempBatchName := uuid.NewString()

			for _, chunkData := range batchChunks {
				// Reads chunk from DB
				chunk, _, err := dbReader.GetSchemaRecordChunk(schema, backupMetadata.ChunkSize, chunkData.Cursor)
				if err != nil {
					uc.logger.Errorf("could not retrieve record chunk from %s schema: %v\n", schema.GetName(), err)
					return false
				}

				// Saves chunks in backup temporal batch file
				if err := backupWriter.SaveSchemaRecordChunk(tempBatchName, chunk); err != nil {
					uc.logger.Errorf("could not save record chunk in backup: %v\n", err)
					return false
				}
			}

			if err := backupWriter.SaveSchemaRecordBatch(tempBatchName, batchHash); err != nil {
				uc.logger.Errorf("could not save record batch in backup: %v\n", err)
				return false
			}

			snapshotData.Data = append(snapshotData.Data, batchHash)
		}

		savedRecords += currentBatchSize
		batchIndex++
	}
	fmt.Println("  - All schema records updated successfully")

	snapshot.Data[schema.GetName()] = snapshotData
	return true
}
