package usecases

import (
	"errors"
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/helpers"
	"historydb/src/internal/services"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

type BackupUsecases interface {
	CreateSnapshot(newBackup bool) *entities.BackupSnapshot
	GetBackupMetadata() *entities.BackupMetadata
	BackupSchemaDependencies(snapshot *entities.BackupSnapshot) bool
	SnapshotSchemaDependencies(lastSnapshot, snapshot *entities.BackupSnapshot) bool
	BackupSchemas(snapshot *entities.BackupSnapshot) bool
	SnapshotSchemas(lastSnapshot, snapshot *entities.BackupSnapshot) bool
	CommitSnapshot(backupMetadata *entities.BackupMetadata, snapshot *entities.BackupSnapshot, isNewBackup bool) bool
}

type BackupUsecasesImpl struct {
	dbFactory     services.DatabaseFactory
	backupFactory services.BackupFactory
	logger        *logrus.Logger
}

func NewBackupUsecasesImpl(dbFactory services.DatabaseFactory, backupFactory services.BackupFactory, logger *logrus.Logger) *BackupUsecasesImpl {
	return &BackupUsecasesImpl{dbFactory, backupFactory, logger}
}

// Creates a new snapshot for the backup
// - If the backup does not exist it initializes the backup structure
func (uc *BackupUsecasesImpl) CreateSnapshot(newBackup bool) *entities.BackupSnapshot {
	backupWriter := uc.backupFactory.CreateWriter()

	snapshot := entities.BackupSnapshot{Id: uuid.NewString(), Timestamp: time.Now(), SchemaDependencies: make(map[string]string), Schemas: make(map[string]string)}
	if newBackup {
		if err := backupWriter.CreateBackupStructure(); err != nil {
			if errors.Is(err, services.ErrBackupDirExists) {
				fmt.Println("The specified path already exists.")
				fmt.Println("To create a new backup you need to provided a non existing path, that will be created.")
				return nil
			}

			uc.logger.Errorf("could not create backup directory: %v\n", err)
			return nil
		}
		fmt.Println("Created backup directory")
	}

	return &snapshot
}

// Returns the existent backup metadata and checks if the backup DB engine matches with the provided DB
func (uc *BackupUsecasesImpl) GetBackupMetadata() *entities.BackupMetadata {
	backupReader := uc.backupFactory.CreateReader()

	backupMetadata, err := backupReader.ReadBackupMetadata()
	if err != nil {
		fmt.Println("The specified path does not contain a backup")
		return nil
	}
	if !uc.dbFactory.CheckBackupDB(backupMetadata.Database) {
		fmt.Println("The database and backup engines does not match")
		return nil
	}

	return &backupMetadata
}

// Makes the first backup for all the schema dependencies
func (uc *BackupUsecasesImpl) BackupSchemaDependencies(snapshot *entities.BackupSnapshot) bool {
	dbReader := uc.dbFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// List schema dependencies from database
	schemaDependencies, err := dbReader.ListSchemaDependencies()
	if err != nil {
		uc.logger.Errorf("could not list schema dependencies from DB: %v\n", err)
		uc.cleanBackup(backupWriter)
		return false
	}

	// Backups all schema dependencies
	schemaDependenciesProgress := progressbar.NewOptions(len(schemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema dependencies...", len(schemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, dependency := range schemaDependencies {
		// Saves dependency into backup
		if err := backupWriter.WriteSchemaDependency(snapshot.Id, dependency); err != nil {
			uc.logger.Errorf("could not save %s schema dependency into backup: %v\n", dependency.GetName(), err)
			uc.cleanBackup(backupWriter)
			return false
		}

		// Saves ref into snapshot
		hash, err := dependency.Hash()
		if err != nil {
			uc.logger.Errorf("could not hash %s schema dependency: %v\n", dependency.GetName(), err)
			uc.cleanBackup(backupWriter)
			return false
		}
		snapshot.SchemaDependencies[dependency.GetName()] = hash
		schemaDependenciesProgress.Add(1)
	}
	fmt.Println("  - All schema dependencies saved successfully")
	return true
}

// Snapshot the schema dependencies changes into the backup
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

	// Updates all schema dependencies
	schemaDependenciesProgress := progressbar.NewOptions(len(schemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema dependencies...", len(schemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, dependency := range schemaDependencies {
		hash, err := dependency.Hash()
		if err != nil {
			uc.logger.Errorf("could not hash %s schema dependency: %v\n", dependency.GetName(), err)
			uc.rollbackBackup(snapshot.Id, backupWriter)
			return false
		}

		// If no condition it stays unmodified
		prevHash, ok := lastSnapshot.SchemaDependencies[dependency.GetName()]
		if !ok {
			// New dependency to write into backup
			if err := backupWriter.WriteSchemaDependency(snapshot.Id, dependency); err != nil {
				uc.logger.Errorf("could not save %s schema dependency into backup: %v\n", dependency.GetName(), err)
				uc.rollbackBackup(snapshot.Id, backupWriter)
				return false
			}
		} else if !helpers.CompareHashes(prevHash, hash) {
			// Updates dependency into backup
			prevDependency, err := backupReader.ReadSchemaDependency(prevHash)
			if err != nil {
				uc.logger.Errorf("could not read %s schema dependency from backup: %v\n", dependency.GetName(), err)
				uc.rollbackBackup(snapshot.Id, backupWriter)
				return false
			}

			dependencyDiff := dependency.Diff(prevDependency)
			if err := backupWriter.WriteSchemaDependencyDiff(snapshot.Id, dependencyDiff); err != nil {
				uc.logger.Errorf("could not update %s schema dependency into backup: %v\n", dependency.GetName(), err)
				uc.rollbackBackup(snapshot.Id, backupWriter)
				return false
			}
		}

		snapshot.SchemaDependencies[dependency.GetName()] = hash
		schemaDependenciesProgress.Add(1)
	}
	fmt.Println("  - All schema dependencies updated successfully")
	return true
}

// Makes the first backup for all the schemas
func (uc *BackupUsecasesImpl) BackupSchemas(snapshot *entities.BackupSnapshot) bool {
	dbReader := uc.dbFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// Lists schemas from database
	schemaNames, err := dbReader.ListSchemaNames()
	if err != nil {
		uc.logger.Errorf("could not list schemas from DB: %v\n", err)
		uc.cleanBackup(backupWriter)
		return false
	}
	fmt.Printf("Read %d schemas from DB: %s\n", len(schemaNames), strings.Join(schemaNames, ", "))

	// Backups all schema definitions
	schemaProgress := progressbar.NewOptions(len(schemaNames), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema definitions...", len(schemaNames))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, schemaName := range schemaNames {
		// Reads schema definition
		schema, err := dbReader.GetSchemaDefinition(schemaName)
		if err != nil {
			uc.logger.Errorf("could not get %s schema definition from DB: %v\n", schemaName, err)
			uc.cleanBackup(backupWriter)
			return false
		}

		// Saves schema into the backup
		if err := backupWriter.WriteSchema(snapshot.Id, schema); err != nil {
			uc.logger.Errorf("could not save %s schema definition into backup: %v\n", schemaName, err)
			uc.cleanBackup(backupWriter)
			return false
		}

		// Saves the schema ref into the snapshot
		hash, err := schema.Hash()
		if err != nil {
			uc.logger.Errorf("could not hash %s schema definition: %v\n", schemaName, err)
			uc.cleanBackup(backupWriter)
			return false
		}
		snapshot.Schemas[schemaName] = hash
		schemaProgress.Add(1)
	}
	fmt.Println("  - All schema definitios saved successfully")
	return true
}

// Snapshot the schema changes into the backup
func (uc *BackupUsecasesImpl) SnapshotSchemas(lastSnapshot, snapshot *entities.BackupSnapshot) bool {
	dbReader := uc.dbFactory.CreateReader()
	backupReader := uc.backupFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// List schemas from database
	schemaNames, err := dbReader.ListSchemaNames()
	if err != nil {
		uc.logger.Errorf("could not list schemas from DB: %v\n", err)
		uc.rollbackBackup(snapshot.Id, backupWriter)
		return false
	}

	// Updates all schema definitions
	schemaProgress := progressbar.NewOptions(len(schemaNames), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema definitions...", len(schemaNames))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, schemaName := range schemaNames {
		// Read schema definition
		schema, err := dbReader.GetSchemaDefinition(schemaName)
		if err != nil {
			uc.logger.Errorf("could not get %s schema definition from DB: %v\n", schemaName, err)
			uc.rollbackBackup(snapshot.Id, backupWriter)
			return false
		}

		hash, err := schema.Hash()
		if err != nil {
			uc.logger.Errorf("could not hash %s schema definition: %v\n", schemaName, err)
			uc.rollbackBackup(snapshot.Id, backupWriter)
			return false
		}

		// If no condition it stays unmodified
		prevHash, ok := lastSnapshot.Schemas[schemaName]
		if !ok {
			// New schema to write into backup
			if err := backupWriter.WriteSchema(snapshot.Id, schema); err != nil {
				uc.logger.Errorf("could not save %s schema definition into backup: %v\n", schemaName, err)
				uc.rollbackBackup(snapshot.Id, backupWriter)
				return false
			}
		} else if !helpers.CompareHashes(prevHash, hash) {
			// Updates schema into backup
			prevSchema, err := backupReader.ReadSchema(prevHash)
			if err != nil {
				uc.logger.Errorf("could not read %s schema definition from backup: %v\n", schemaName, err)
				uc.rollbackBackup(snapshot.Id, backupWriter)
				return false
			}

			schemaDiff := schema.Diff(prevSchema)
			if err := backupWriter.WriteSchemaDiff(snapshot.Id, schemaDiff); err != nil {
				uc.logger.Errorf("could not update %s schema dependency into backup: %v\n", schemaName, err)
				uc.rollbackBackup(snapshot.Id, backupWriter)
				return false
			}
		}

		snapshot.Schemas[schemaName] = hash
		schemaProgress.Add(1)
	}
	fmt.Println("  - All schema definitios updated successfully")
	return true
}

// Commits the snapshot into the backup metadata
func (uc *BackupUsecasesImpl) CommitSnapshot(backupMetadata *entities.BackupMetadata, snapshot *entities.BackupSnapshot, isNewBackup bool) bool {
	backupWriter := uc.backupFactory.CreateWriter()

	if backupMetadata == nil {
		backupMetadata = &entities.BackupMetadata{Database: uc.dbFactory.GetDBMetadata(), Snapshots: []entities.BackupSnapshot{*snapshot}}
	} else {
		backupMetadata.Snapshots = append(backupMetadata.Snapshots, *snapshot)
	}

	if err := backupWriter.CommitSnapshot(snapshot.Id, *backupMetadata); err != nil {
		uc.logger.Errorf("could not save snapshot: %v\n", err)
		if isNewBackup {
			uc.cleanBackup(backupWriter)
		} else {
			uc.rollbackBackup(snapshot.Id, backupWriter)
		}
		return false
	}
	fmt.Println("Backup saved successfully")
	return true
}

// Cleans the backup structure when the first backup snapshot fails
func (uc *BackupUsecasesImpl) cleanBackup(backupWriter services.BackupWriter) {
	fmt.Println("Process failed. Aborting operation...")
	fmt.Println("  - Cleaning backup directory...")

	if err := backupWriter.DeleteBackupStructure(); err != nil {
		uc.logger.Errorf("could not delete backup directory: %v\n", err)
		return
	}

	fmt.Println("  + Clean successful")
	fmt.Println("Closing app...")
}

// Rollbacks the backup to the previous stabel snapshot if the current snapshot fails
func (uc *BackupUsecasesImpl) rollbackBackup(idSnapshot string, backupWriter services.BackupWriter) {
	fmt.Println("Process failed. Aborting operation...")
	fmt.Println("  - Rollback to previous state...")

	if err := backupWriter.RollbackSnapshot(idSnapshot); err != nil {
		uc.logger.Errorf("could not rollback to previous state: %v\n", err)
		return
	}

	fmt.Println("  + Rollback completed")
	fmt.Println("Closing app...")
}
