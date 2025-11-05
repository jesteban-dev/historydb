package usecases

import (
	"errors"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	backup_services "historydb/src/internal/services/backup"
	database_services "historydb/src/internal/services/database"
	"historydb/src/internal/utils/types"
	"os"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

type RestoreUsecasesImpl struct {
	dbFactory     database_services.DatabaseFactory
	backupFactory backup_services.BackupFactory
	logger        *logrus.Logger
}

func NewRestoreUsecasesImpl(dbFactory database_services.DatabaseFactory, backupFactory backup_services.BackupFactory, logger *logrus.Logger) *RestoreUsecasesImpl {
	return &RestoreUsecasesImpl{dbFactory, backupFactory, logger}
}

func (uc *RestoreUsecasesImpl) GetBackupSnapshot(snapshotId *string) *entities.BackupSnapshot {
	backupReader := uc.backupFactory.CreateReader()
	dbReader := uc.dbFactory.CreateReader()

	// Checking if DB is empty to dump all the backup content
	if isEmpty, err := dbReader.CheckDBIsEmpty(); err != nil {
		uc.logger.Errorf("could not check if database is empty: %v\n", err)
		return nil
	} else if !isEmpty {
		fmt.Println("To restore the database it is required an empty database")
		return nil
	}

	// Reads metadata backup and checks engines
	backupMetadata, err := backupReader.GetBackupMetadata()
	if err != nil {
		fmt.Println("The specified path does not seem to contain a backup")
		uc.logger.Errorf("could not retrieve backup: %v\n", err)
		return nil
	}

	if uc.dbFactory.GetDBEngine() != backupMetadata.DatabaseEngine {
		fmt.Println("The database and backup engines does not match")
		return nil
	}

	// If the user specified a snapshot it gets that snapshot from where to restore the DB. If not it selects the last one
	var snapshot entities.BackupSnapshot
	if snapshotId != nil {
		timestamp, err := time.Parse(time.RFC3339, *snapshotId)
		if err != nil {
			for _, item := range backupMetadata.Snapshots {
				if item.SnapshotId == *snapshotId {
					snapshot, err = backupReader.GetBackupSnapshot(item.SnapshotId)
					if err != nil {
						uc.logger.Errorf("could not retrieve snapshot from backup: %v\n", err)
						return nil
					}
					break
				}
			}
		} else {
			for _, item := range backupMetadata.Snapshots {
				if item.Timestamp.Equal(timestamp) {
					snapshot, err = backupReader.GetBackupSnapshot(item.SnapshotId)
					if err != nil {
						uc.logger.Errorf("could not retrieve snapshot from backup: %v\n", err)
						return nil
					}
					break
				}
			}
		}
	} else {
		snapshot, err = backupReader.GetBackupSnapshot(backupMetadata.Snapshots[len(backupMetadata.Snapshots)-1].SnapshotId)
		if err != nil {
			uc.logger.Errorf("could not retrieve snapshot from backup: %v\n", err)
			return nil
		}
	}

	return &snapshot
}

func (uc *RestoreUsecasesImpl) BeginDatabaseRestore() bool {
	dbWriter := uc.dbFactory.CreateWriter()

	if err := dbWriter.BeginTransaction(); err != nil {
		uc.logger.Errorf("could not begin DB transaction: %v\n", err)
		return false
	}
	return true
}

func (uc *RestoreUsecasesImpl) CommitDatabaseRestore() bool {
	dbWriter := uc.dbFactory.CreateWriter()

	if err := dbWriter.CommitTransaction(); err != nil {
		uc.logger.Errorf("could not commit restored DB: %v\n", err)
		return false
	}
	return true
}

func (uc *RestoreUsecasesImpl) RollbackDatabaseRestore() {
	dbWriter := uc.dbFactory.CreateWriter()
	fmt.Println("Process failed. Aborting operation...")
	fmt.Println("  - Rollback DB to previous state...")

	if err := dbWriter.RollbackTransaction(); err != nil {
		uc.logger.Errorf("could not rollback DB to previous state: %v\n", err)
		return
	}

	fmt.Println("  + Rollback completed!")
	fmt.Println("Closing app...")
}

func (uc *RestoreUsecasesImpl) RestoreSchemaDependencies(snapshot *entities.BackupSnapshot) bool {
	backupReader := uc.backupFactory.CreateReader()
	dbWriter := uc.dbFactory.CreateWriter()

	schemaDependenciesProgress := progressbar.NewOptions(len(snapshot.SchemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Restoring all %d schema dependencies...", len(snapshot.SchemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for dependencyName, snapshotDependency := range snapshot.SchemaDependencies {
		dependency, _, err := backupReader.GetSchemaDependency(snapshotDependency)
		if err != nil {
			if errors.Is(err, services.ErrBackupCorruptedFile) {
				fmt.Printf("The %s schema dependency in backup is corrupted\n", dependencyName)
			}

			uc.logger.Errorf("could not read %s schema dependency from backup: %v\n", dependencyName, err)
			return false
		}

		if err := dbWriter.SaveSchemaDependency(dependency); err != nil {
			uc.logger.Errorf("could not restore %s schema dependency: %v\n", dependencyName, err)
			return false
		}

		schemaDependenciesProgress.Add(1)
	}

	fmt.Println("  - All schema dependencies restored successfully")
	return true
}

func (uc *RestoreUsecasesImpl) RestoreSchemas(snapshot *entities.BackupSnapshot) []entities.Schema {
	backupReader := uc.backupFactory.CreateReader()
	dbWriter := uc.dbFactory.CreateWriter()

	schemas := make([]entities.Schema, 0, len(snapshot.Schemas))
	schemaProgress := progressbar.NewOptions(len(snapshot.Schemas), progressbar.OptionSetDescription(fmt.Sprintf("  + Restoring all %d schemas...", len(snapshot.Schemas))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for schemaName, snapshotSchema := range snapshot.Schemas {
		schema, _, err := backupReader.GetSchema(snapshotSchema)
		if err != nil {
			if errors.Is(err, services.ErrBackupCorruptedFile) {
				fmt.Printf("The %s schema in backup is corrupted\n", schemaName)
			}

			uc.logger.Errorf("could not read %s schema from backup: %v\n", schemaName, err)
			return nil
		}

		if err := dbWriter.SaveSchema(schema); err != nil {
			uc.logger.Errorf("could not restore %s schema: %v\n", schemaName, err)
			return nil
		}

		schemas = append(schemas, schema)
		schemaProgress.Add(1)
	}
	fmt.Println("  - All schemas restored successfully")

	return schemas
}

func (uc *RestoreUsecasesImpl) RestoreSchemaRules(snapshot *entities.BackupSnapshot, schemas []entities.Schema) bool {
	dbWriter := uc.dbFactory.CreateWriter()

	fmt.Println("  + Restoring schema rules...")
	for _, schema := range schemas {
		if err := dbWriter.SaveSchemaRules(schema); err != nil {
			uc.logger.Errorf("could not restore %s schema rules: %v\n", schema.GetName(), err)
			return false
		}
	}

	fmt.Println("  - All schema rules restored successfully")
	return true
}

func (uc *RestoreUsecasesImpl) RestoreSchemaRecords(snapshot *entities.BackupSnapshot, schema entities.Schema) bool {
	backupReader := uc.backupFactory.CreateReader()
	dbWriter := uc.dbFactory.CreateWriter()

	backupMetadata, ok := snapshot.Data[schema.GetName()]
	if !ok {
		uc.logger.Errorf("%s schema is not present in backup records\n", schema.GetName())
		return false
	}

	batchProgress := progressbar.NewOptions(len(backupMetadata.Data), progressbar.OptionSetDescription(fmt.Sprintf("  + Restoring all %d batches for %s schema...", len(backupMetadata.Data), schema.GetName())), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	// Loops over every schema batch
	for _, batch := range backupMetadata.Data {
		// Retrieves chunk references in the batch
		chunkRefs, err := backupReader.GetSchemaRecordChunkRefsInBatch(batch)
		if err != nil {
			uc.logger.Errorf("could not retrieve chunk refs from %s schema: %v\n", schema.GetName(), err)
			return false
		}

		// Loops over every chunk reference in the batch
		for _, chunkRef := range chunkRefs {
			// Retrieves the chunk from the reference
			chunk, _, err := backupReader.GetSchemaRecordChunk(batch, chunkRef)
			if err != nil {
				uc.logger.Errorf("could not retrieve chunk from %s schema: %v\n", schema.GetName(), err)
				return false
			}

			// Saves the chunk into DB
			if err := dbWriter.SaveSchemaRecords(schema, chunk); err != nil {
				uc.logger.Errorf("could not save records in DB %s schema: %v\n", schema.GetName(), err)
				return false
			}
		}
		batchProgress.Add(1)
	}

	fmt.Println("  - All batches restored successfully")
	return true
}

// Hay que incluir el orden de las dependencias
func (uc *RestoreUsecasesImpl) RestoreRoutines(snapshot *entities.BackupSnapshot) bool {
	backupReader := uc.backupFactory.CreateReader()
	dbWriter := uc.dbFactory.CreateWriter()

	restoredDependencies := []string{}
	routineProgress := progressbar.NewOptions(len(snapshot.Routines), progressbar.OptionSetDescription(fmt.Sprintf("  + Restoring all %d routines...", len(snapshot.Routines))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for routineName, snapshotRoutine := range snapshot.Routines {
		if ok := types.SeachInSlice(restoredDependencies, snapshotRoutine); !ok {
			newRestoredDependencies, ok := uc.restoreSingleRoutine(snapshot, backupReader, dbWriter, routineName, snapshotRoutine)
			if !ok {
				return false
			}

			restoredDependencies = append(restoredDependencies, newRestoredDependencies...)
			restoredDependencies = append(restoredDependencies, snapshotRoutine)

			routineProgress.Add(len(newRestoredDependencies) + 1)
		}
	}

	fmt.Println("  - All routines restored successfully")
	return true
}

func (uc *RestoreUsecasesImpl) restoreSingleRoutine(snapshot *entities.BackupSnapshot, backupReader backup_services.BackupReader, dbWriter database_services.DatabaseWriter, routineName, routineRef string) ([]string, bool) {
	restoredDependencies := []string{}

	routine, _, err := backupReader.GetRoutine(routineRef)
	if err != nil {
		if errors.Is(err, services.ErrBackupCorruptedFile) {
			fmt.Printf("The %s routine in backup is corrupted\n", routineName)
		}

		uc.logger.Errorf("could not read %s routine from backup: %v\n", routineName, err)
		return nil, false
	}

	for _, dependency := range routine.GetDependencies() {
		snapshotDependency, ok := snapshot.Routines[dependency]
		if !ok {
			uc.logger.Errorf("could not find %s routine in backup\n", dependency)
			return nil, false
		}

		if ok := types.SeachInSlice(restoredDependencies, snapshotDependency); !ok {
			newRestoredDependencies, ok := uc.restoreSingleRoutine(snapshot, backupReader, dbWriter, dependency, snapshotDependency)
			if !ok {
				return nil, false
			}
			restoredDependencies = append(restoredDependencies, newRestoredDependencies...)
			restoredDependencies = append(restoredDependencies, snapshotDependency)
		}
	}

	if err := dbWriter.SaveRoutine(routine); err != nil {
		uc.logger.Errorf("could not restore %s routine: %v\n", routineName, err)
		return nil, false
	}

	return restoredDependencies, true
}
