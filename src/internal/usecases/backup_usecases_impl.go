package usecases

import (
	"errors"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	backup_services "historydb/src/internal/services/backup"
	database_services "historydb/src/internal/services/database"
	"historydb/src/internal/utils/crypto"
	"os"
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

func (uc *BackupUsecasesImpl) BackupSchemaDependencies(lastSnapshot, snapshot *entities.BackupSnapshot) bool {
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
		if lastSnapshot != nil {
			backupReader := uc.backupFactory.CreateReader()

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
				prevDependency, err := backupReader.GetSchemaDependency(prevHash)
				if err != nil {
					if errors.Is(err, services.ErrBackupCorruptedFile) {
						fmt.Printf("The %s schema dependency in backup is corrupted\n", dependency.GetName())
					}
					uc.logger.Errorf("could not read %s schema dependency from backup: %v\n", dependency.GetName(), err)
					return false
				}

				dependencyDiff := dependency.Diff(prevDependency)
				if err := backupWriter.SaveSchemaDependencyDiff(dependencyDiff); err != nil {
					uc.logger.Errorf("could not update %s schema dependency into backup: %v\n", dependency.GetName(), err)
					return false
				}

				snapshot.SchemaDependencies[dependency.GetName()] = fmt.Sprintf("diffs/%s", hash)
			}
		} else {
			// Saves dependency into backup
			if err := backupWriter.SaveSchemaDependency(dependency); err != nil {
				uc.logger.Errorf("could not save %s schema dependency into backup: %v\n", dependency.GetName(), err)
				return false
			}

			// Saves ref into snapshot
			snapshot.SchemaDependencies[dependency.GetName()] = dependency.Hash()
		}
		schemaDependenciesProgress.Add(1)
	}

	fmt.Println("  - All schema dependencies saved successfully!")
	return true
}

func (uc *BackupUsecasesImpl) BackupSchemas(lastSnapshot, snapshot *entities.BackupSnapshot) []entities.Schema {
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

		if lastSnapshot != nil {
			backupReader := uc.backupFactory.CreateReader()

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
				prevSchema, err := backupReader.GetSchema(prevHash)
				if err != nil {
					if errors.Is(err, services.ErrBackupCorruptedFile) {
						fmt.Printf("The %s schema in backup is corrupted\n", schema.GetName())
					}

					uc.logger.Errorf("could not read %s schema definition from backup: %v\n", schemaName, err)
					return nil
				}

				schemaDiff := schema.Diff(prevSchema)
				if err := backupWriter.SaveSchemaDiff(schemaDiff); err != nil {
					uc.logger.Errorf("could not update %s schema definition into backup: %v\n", schemaName, err)
					return nil
				}

				snapshot.Schemas[schemaName] = fmt.Sprintf("diffs/%s", hash)
			}
		} else {
			schemas = append(schemas, schema)

			// Saves schema into the backup
			if err := backupWriter.SaveSchema(schema); err != nil {
				uc.logger.Errorf("could not save %s schema definition into backup: %v\n", schemaName, err)
				return nil
			}

			// Saves the schema ref into the snapshot
			snapshot.Schemas[schemaName] = hash
		}
		schemaProgress.Add(1)
	}
	fmt.Println("  - All schema definitions saved successfully")
	return schemas
}
