package usecases

import (
	"fmt"
	"historydb/src/internal/entities"
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

func (uc *BackupUsecases) CreateBackup() {
	dbReader := uc.dbFactory.CreateReader()
	backupWriter := uc.backupFactory.CreateWriter()

	snapshot := entities.Snapshot{
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
		progressbar.OptionSetDescription(fmt.Sprintf("Saving all %d schema definitions...", len(schemaNames))),
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

		if err := backupWriter.WriteSchema(schema); err != nil {
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

	if err := backupWriter.CommitSnapshotList([]entities.Snapshot{snapshot}); err != nil {
		uc.logger.Error("impossible to write snapshot log", "error", err.Error())
		uc.cleanAbortingBackup(backupWriter)
		return
	}
	fmt.Println("\nBackup saved successfully.")
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
