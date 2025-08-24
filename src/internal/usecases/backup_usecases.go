package usecases

import (
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/helpers"
	"historydb/src/internal/services"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/schollz/progressbar/v3"
)

type BackupUsecases struct {
	dbFactory     services.DatabaseFactory
	backupFactory services.BackupFactory
	logger        *slog.Logger
}

func NewBackupUsecases(dbFactory services.DatabaseFactory, backupFactory services.BackupFactory, logger *slog.Logger) *BackupUsecases {
	return &BackupUsecases{dbFactory, backupFactory, logger}
}

// CreateBackup is the usecase used for create the first backup snapshot.
func (uc *BackupUsecases) CreateBackup() {
	dbReader := uc.dbFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	snapshot := entities.BackupSnapshot{
		Id:        uuid.NewString(),
		Timestamp: time.Now(),
		Schemas:   make(map[string]string),
	}

	if err := backupWriter.CreateBackupStructure(); err != nil {
		uc.logger.Error(err.Error())
		return
	}
	fmt.Println("Created backup directory")

	schemaNames, err := dbReader.ListSchemaNames()
	if err != nil {
		uc.logger.Error("impossible to list schema names from database", "error", err.Error())
		uc.cleanAbortingBackup(backupWriter)
		return
	}
	fmt.Printf("\nSuccessfully read %d schemas: %s\n", len(schemaNames), strings.Join(schemaNames, ", "))

	schemaDefinitionBar := progressbar.NewOptions(
		len(schemaNames),
		progressbar.OptionSetDescription(fmt.Sprintf("  + Saving all %d schema definitions...", len(schemaNames))),
		progressbar.OptionSetWidth(30),
		progressbar.OptionSetWriter(os.Stdout),
		progressbar.OptionSetRenderBlankState(true),
	)
	for _, name := range schemaNames {
		schema, err := dbReader.GetSchemaDefinition(name)
		if err != nil {
			uc.logger.Error("impossible to get schema from database", "error", err.Error())
			uc.cleanAbortingBackup(backupWriter)
			return
		}

		if err := backupWriter.WriteSchema(snapshot.Id, schema); err != nil {
			uc.logger.Error("impossible to write schema into backup", "error", err.Error())
			uc.cleanAbortingBackup(backupWriter)
			return
		}

		hash, err := schema.Hash()
		if err != nil {
			uc.logger.Error("impossible to write schema into backup", "error", err.Error())
			uc.cleanAbortingBackup(backupWriter)
			return
		}
		snapshot.Schemas[name] = hash
		schemaDefinitionBar.Add(1)
	}
	fmt.Printf("\n  - All schema definitions saved successfully\n")

	backupMetadata := entities.BackupMetadata{
		Database:  uc.dbFactory.GetDBMetadata(),
		Snapshots: []entities.BackupSnapshot{snapshot},
	}

	if err := backupWriter.CommitSnapshot(snapshot.Id, backupMetadata); err != nil {
		uc.logger.Error("impossible to write snapshot log", "error", err.Error())
		uc.cleanAbortingBackup(backupWriter)
		return
	}
	fmt.Println("\nBackup saved successfully.")
}

// SnapshotBackup is the usecase used for taken a new snapshot from the DB and update the backup.
func (uc *BackupUsecases) SnapshotBackup() {
	backupReader := uc.backupFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()
	dbReader := uc.dbFactory.CreateReader()

	metadata, err := backupReader.ReadBackupMetadata()
	if err != nil {
		uc.logger.Error("the backup does not exist", "error", err.Error())
		return
	}

	if !uc.dbFactory.CheckBackupDB(metadata.Database) {
		fmt.Println("The database engine does not match with the backup")
		return
	}
	lastSnapshot := metadata.Snapshots[len(metadata.Snapshots)-1]
	newSnapshot := entities.BackupSnapshot{
		Id:        uuid.NewString(),
		Timestamp: time.Now(),
		Schemas:   map[string]string{},
	}

	schemaNames, err := dbReader.ListSchemaNames()
	if err != nil {
		uc.logger.Error("impossible to list schema names from database", "error", err.Error())
		return
	}
	fmt.Printf("Successfully read %d schemas: %s\n", len(schemaNames), strings.Join(schemaNames, ", "))

	schemaDefinitionBar := progressbar.NewOptions(
		len(schemaNames),
		progressbar.OptionSetDescription(fmt.Sprintf("  + Updating all %d schema definitions...", len(schemaNames))),
		progressbar.OptionSetWidth(30),
		progressbar.OptionSetWriter(os.Stdout),
		progressbar.OptionSetRenderBlankState(true),
	)
	for _, schemaName := range schemaNames {
		schema, err := dbReader.GetSchemaDefinition(schemaName)
		if err != nil {
			uc.logger.Error("impossible to get schema from database", "error", err.Error())
			uc.rollbackSnapshot(newSnapshot.Id, backupWriter)
			return
		}

		hash, err := schema.Hash()
		if err != nil {
			uc.logger.Error("impossible to generate schema hash", "error", err)
			uc.rollbackSnapshot(newSnapshot.Id, backupWriter)
			return
		}

		prevHash, ok := lastSnapshot.Schemas[schemaName]
		if !ok {
			if err := backupWriter.WriteSchema(newSnapshot.Id, schema); err != nil {
				uc.logger.Error("impossible to write schema into backup", "error", err.Error())
				uc.rollbackSnapshot(newSnapshot.Id, backupWriter)
				return
			}
			newSnapshot.Schemas[schemaName] = hash
		} else if !helpers.CompareHashes(prevHash, hash) {
			prevSchema, err := backupReader.ReadSchema(prevHash)
			if err != nil {
				uc.logger.Error("impossible to read schema from backup", "error", err.Error())
				uc.rollbackSnapshot(newSnapshot.Id, backupWriter)
				return
			}

			schemaDiff := schema.Diff(prevSchema)
			if err := backupWriter.WriteSchemaDiff(newSnapshot.Id, schemaDiff); err != nil {
				uc.logger.Error("impossible to save schema updates", "error", err.Error())
				uc.rollbackSnapshot(newSnapshot.Id, backupWriter)
				return
			}
			newSnapshot.Schemas[schemaName] = hash
		} else {
			newSnapshot.Schemas[schemaName] = prevHash
		}

		schemaDefinitionBar.Add(1)
	}
	fmt.Println("\n  - All schemas definitions updated successfully")

	metadata.Snapshots = append(metadata.Snapshots, newSnapshot)
	if err := backupWriter.CommitSnapshot(newSnapshot.Id, metadata); err != nil {
		uc.logger.Error("impossible to write snapshot log", "error", err.Error())
		uc.rollbackSnapshot(newSnapshot.Id, backupWriter)
		return
	}

	fmt.Println("\nBackup snapshot saved successfully")
}

func (uc *BackupUsecases) cleanAbortingBackup(backupWriter services.BackupWriter) {
	fmt.Println("Aborting operation...")
	fmt.Println("  - Cleaning backup directory...")

	if err := backupWriter.DeleteBackupStructure(); err != nil {
		uc.logger.Error("impossible to clean backup directory", "error", err.Error())
	} else {
		fmt.Println("  + Clean successful")
	}

	fmt.Println("Closing app...")
}

func (uc *BackupUsecases) rollbackSnapshot(uuidSnapshot string, backupWriter services.BackupWriter) {
	fmt.Println("Aborting operation...")
	fmt.Println("  - Rollback to previous state...")

	if err := backupWriter.RollbackSnapshot(uuidSnapshot); err != nil {
		uc.logger.Error("impossible to rollback to previous state", "error", err.Error())
	} else {
		fmt.Println("  + Rollback completed")
	}

	fmt.Println("Closing app...")
}
