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

type BackupUsecases struct {
	dbFactory     services.DatabaseFactory
	backupFactory services.BackupFactory
	logger        *logrus.Logger
}

func NewBackupUsecases(dbFactory services.DatabaseFactory, backupFactory services.BackupFactory, logger *logrus.Logger) *BackupUsecases {
	return &BackupUsecases{dbFactory, backupFactory, logger}
}

// CreateBackup is the usecase used for create the first snapshot in our backup.
func (uc *BackupUsecases) CreateBackup() {
	dbReader := uc.dbFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	initSnapshot := entities.BackupSnapshot{
		Id:                 uuid.NewString(),
		Timestamp:          time.Now(),
		SchemaDependencies: make(map[string]string),
		Schemas:            make(map[string]string),
	}

	// Creates the backup directory structure
	if err := backupWriter.CreateBackupStructure(); err != nil {
		if errors.Is(err, services.ErrBackupDirExists) {
			fmt.Println("The specified path already exists.")
			fmt.Println("To create a new backup you need to provided a non existing path, that will be created.")
			return
		}

		uc.logger.Errorf("could not create backup directory: %v\n", err)
		return
	}
	fmt.Println("Created backup directory")

	// List schema dependencies from database
	schemaDependencies, err := dbReader.ListSchemaDependencies()
	if err != nil {
		uc.logger.Errorf("could not list schema dependencies from DB: %v\n", err)
		uc.cleanBackup(backupWriter)
		return
	}

	// Backups all schema dependencies
	schemaDependenciesProgress := progressbar.NewOptions(len(schemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema dependencies...", len(schemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, dependency := range schemaDependencies {
		// Saves dependency into backup
		if err := backupWriter.WriteSchemaDependency(initSnapshot.Id, dependency); err != nil {
			uc.logger.Errorf("could not save %s schema dependency into backup: %v\n", dependency.GetName(), err)
			uc.cleanBackup(backupWriter)
			return
		}

		// Saves ref into snapshot
		hash, err := dependency.Hash()
		if err != nil {
			uc.logger.Errorf("could not hash %s schema dependency: %v\n", dependency.GetName(), err)
			uc.cleanBackup(backupWriter)
			return
		}
		initSnapshot.SchemaDependencies[dependency.GetName()] = hash
		schemaDependenciesProgress.Add(1)
	}
	fmt.Println("  - All schema dependencies saved successfully")

	// Lists schemas from database
	schemaNames, err := dbReader.ListSchemaNames()
	if err != nil {
		uc.logger.Errorf("could not list schemas from DB: %v\n", err)
		uc.cleanBackup(backupWriter)
		return
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
			return
		}

		// Saves schema into the backup
		if err := backupWriter.WriteSchema(initSnapshot.Id, schema); err != nil {
			uc.logger.Errorf("could not save %s schema definition into backup: %v\n", schemaName, err)
			uc.cleanBackup(backupWriter)
			return
		}

		// Saves the schema ref into the snapshot
		hash, err := schema.Hash()
		if err != nil {
			uc.logger.Errorf("could not hash %s schema definition: %v\n", schemaName, err)
			uc.cleanBackup(backupWriter)
			return
		}
		initSnapshot.Schemas[schemaName] = hash
		schemaProgress.Add(1)
	}
	fmt.Println("  - All schema definitios saved successfully")

	// Saves backup snapshot and DB engine
	backupMetadata := entities.BackupMetadata{Database: uc.dbFactory.GetDBMetadata(), Snapshots: []entities.BackupSnapshot{initSnapshot}}
	if err := backupWriter.CommitSnapshot(initSnapshot.Id, backupMetadata); err != nil {
		uc.logger.Errorf("could not save snapshot: %v\n", err)
		uc.cleanBackup(backupWriter)
		return
	}
	fmt.Println("Backup saved successfully")
}

func (uc *BackupUsecases) cleanBackup(backupWriter services.BackupWriter) {
	fmt.Println("Process failed. Aborting operation...")
	fmt.Println("  - Cleaning backup directory...")

	if err := backupWriter.DeleteBackupStructure(); err != nil {
		uc.logger.Errorf("could not delete backup directory: %v\n", err)
		return
	}

	fmt.Println("  + Clean successful")
	fmt.Println("Closing app...")
}

func (uc *BackupUsecases) SnapshotBackup() {
	dbReader := uc.dbFactory.CreateReader()
	backupReader := uc.backupFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	// Reads metadata backup and checks engines
	backupMetadata, err := backupReader.ReadBackupMetadata()
	if err != nil {
		fmt.Println("The specified path does not contain a backup")
		return
	}
	if !uc.dbFactory.CheckBackupDB(backupMetadata.Database) {
		fmt.Println("The database and backup engines does not match")
		return
	}

	// Gets last snapshot where to read previous state, and creates new snapshot
	lastSnapshot := backupMetadata.Snapshots[len(backupMetadata.Snapshots)-1]
	newSnapshot := entities.BackupSnapshot{Id: uuid.NewString(), Timestamp: time.Now(), SchemaDependencies: make(map[string]string), Schemas: make(map[string]string)}

	// List schema dependencies from database
	schemaDependencies, err := dbReader.ListSchemaDependencies()
	if err != nil {
		uc.logger.Errorf("could not list schema dependencies from DB: %v\n", err)
		return
	}

	// Updates all schema dependencies
	schemaDependenciesProgress := progressbar.NewOptions(len(schemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema dependencies...", len(schemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, dependency := range schemaDependencies {
		hash, err := dependency.Hash()
		if err != nil {
			uc.logger.Errorf("could not hash %s schema dependency: %v\n", dependency.GetName(), err)
			uc.rollbackBackup(newSnapshot.Id, backupWriter)
			return
		}

		// If no condition it stays unmodified
		prevHash, ok := lastSnapshot.SchemaDependencies[dependency.GetName()]
		if !ok {
			// New dependency to write into backup
			if err := backupWriter.WriteSchemaDependency(newSnapshot.Id, dependency); err != nil {
				uc.logger.Errorf("could not save %s schema dependency into backup: %v\n", dependency.GetName(), err)
				uc.rollbackBackup(newSnapshot.Id, backupWriter)
				return
			}
		} else if !helpers.CompareHashes(prevHash, hash) {
			// Updates dependency into backup
			prevDependency, err := backupReader.ReadSchemaDependency(prevHash)
			if err != nil {
				uc.logger.Errorf("could not read %s schema dependency from backup: %v\n", dependency.GetName(), err)
				uc.rollbackBackup(newSnapshot.Id, backupWriter)
				return
			}

			dependencyDiff := dependency.Diff(prevDependency)
			if err := backupWriter.WriteSchemaDependencyDiff(newSnapshot.Id, dependencyDiff); err != nil {
				uc.logger.Errorf("could not update %s schema dependency into backup: %v\n", dependency.GetName(), err)
				uc.rollbackBackup(newSnapshot.Id, backupWriter)
				return
			}
		}

		newSnapshot.SchemaDependencies[dependency.GetName()] = hash
		schemaDependenciesProgress.Add(1)
	}
	fmt.Println("  - All schema dependencies updated successfully")

	// List schemas from database
	schemaNames, err := dbReader.ListSchemaNames()
	if err != nil {
		uc.logger.Errorf("could not list schemas from DB: %v\n", err)
		uc.rollbackBackup(newSnapshot.Id, backupWriter)
		return
	}

	// Updates all schema definitions
	schemaProgress := progressbar.NewOptions(len(schemaNames), progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema definitions...", len(schemaNames))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for _, schemaName := range schemaNames {
		// Read schema definition
		schema, err := dbReader.GetSchemaDefinition(schemaName)
		if err != nil {
			uc.logger.Errorf("could not get %s schema definition from DB: %v\n", schemaName, err)
			uc.rollbackBackup(newSnapshot.Id, backupWriter)
			return
		}

		hash, err := schema.Hash()
		if err != nil {
			uc.logger.Errorf("could not hash %s schema definition: %v\n", schemaName, err)
			uc.rollbackBackup(newSnapshot.Id, backupWriter)
			return
		}

		// If no condition it stays unmodified
		prevHash, ok := lastSnapshot.Schemas[schemaName]
		if !ok {
			// New schema to write into backup
			if err := backupWriter.WriteSchema(newSnapshot.Id, schema); err != nil {
				uc.logger.Errorf("could not save %s schema definition into backup: %v\n", schemaName, err)
				uc.rollbackBackup(newSnapshot.Id, backupWriter)
				return
			}
		} else if !helpers.CompareHashes(prevHash, hash) {
			// Updates schema into backup
			prevSchema, err := backupReader.ReadSchema(prevHash)
			if err != nil {
				uc.logger.Errorf("could not read %s schema definition from backup: %v\n", schemaName, err)
				uc.rollbackBackup(newSnapshot.Id, backupWriter)
				return
			}

			schemaDiff := schema.Diff(prevSchema)
			if err := backupWriter.WriteSchemaDiff(newSnapshot.Id, schemaDiff); err != nil {
				uc.logger.Errorf("could not update %s schema dependency into backup: %v\n", schemaName, err)
				uc.rollbackBackup(newSnapshot.Id, backupWriter)
				return
			}
		}

		newSnapshot.Schemas[schemaName] = hash
		schemaProgress.Add(1)
	}
	fmt.Println("  - All schema definitios updated successfully")

	// Save new backup snapshot
	backupMetadata.Snapshots = append(backupMetadata.Snapshots, newSnapshot)
	if err := backupWriter.CommitSnapshot(newSnapshot.Id, backupMetadata); err != nil {
		uc.logger.Errorf("could not save snapshot: %v\n", err)
		uc.rollbackBackup(newSnapshot.Id, backupWriter)
		return
	}
	fmt.Println("Snapshot saved successfully")
}

func (uc *BackupUsecases) rollbackBackup(idSnapshot string, backupWriter services.BackupWriter) {
	fmt.Println("Process failed. Aborting operation...")
	fmt.Println("  - Rollback to previous state...")

	if err := backupWriter.RollbackSnapshot(idSnapshot); err != nil {
		uc.logger.Errorf("could not rollback to previous state: %v\n", err)
		return
	}

	fmt.Println("  + Rollback completed")
	fmt.Println("Closing app...")
}
