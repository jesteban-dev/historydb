package usecases

import (
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	"os"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

type RestoreUsecases struct {
	dbFactory     services.DatabaseFactory
	backupFactory services.BackupFactory
	logger        *logrus.Logger
}

func NewRestoreUsecases(dbFactory services.DatabaseFactory, backupFactory services.BackupFactory, logger *logrus.Logger) *RestoreUsecases {
	return &RestoreUsecases{dbFactory, backupFactory, logger}
}

func (uc *RestoreUsecases) RestoreDatabase(snapshotId *string) {
	backupReader := uc.backupFactory.CreateReader()
	dbReader := uc.dbFactory.CreateReader()
	dbWriter := uc.dbFactory.CreateWriter()

	// Checking if DB is empty to dump all the backup content
	if isEmpty, err := dbReader.CheckDBIsEmpty(); err != nil {
		uc.logger.Errorf("could not check if database is empty: %v\n", err)
		return
	} else if !isEmpty {
		fmt.Println("To restore the database it is required an empty database")
		return
	}

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

	// If the user specified a snapshot it gets that snapshot from where to restore the DB. If not it selects the last one
	var snapshot entities.BackupSnapshot
	if snapshotId != nil {
		timestamp, err := time.Parse(time.RFC3339, *snapshotId)
		if err != nil {
			for _, item := range backupMetadata.Snapshots {
				if item.Id == *snapshotId {
					snapshot = item
				}
			}
		} else {
			for _, item := range backupMetadata.Snapshots {
				if item.Timestamp.Equal(timestamp) {
					snapshot = item
				}
			}
		}
	} else {
		snapshot = backupMetadata.Snapshots[len(backupMetadata.Snapshots)-1]
	}

	if err := dbWriter.BeginTransaction(); err != nil {
		uc.logger.Errorf("could not begin DB transaction: %v\n", err)
		return
	}

	// Restore all schema dependencies
	schemaDependenciesProgress := progressbar.NewOptions(len(snapshot.SchemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Restoring all %d schema dependencies...", len(snapshot.SchemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for dependencyName, dependencyHash := range snapshot.SchemaDependencies {
		dependency, err := backupReader.ReadSchemaDependency(dependencyHash)
		if err != nil {
			uc.logger.Errorf("could not read %s schema dependency from backup: %v\n", dependencyName, err)
			uc.rollbackRestore(dbWriter)
			return
		}

		if err := dbWriter.WriteSchemaDependency(dependency); err != nil {
			uc.logger.Errorf("could not restore %s schema dependency: %v\n", dependencyName, err)
			uc.rollbackRestore(dbWriter)
			return
		}

		schemaDependenciesProgress.Add(1)
	}
	fmt.Println("  - All schema dependencies restored successfully")

	schemas := make([]entities.Schema, 0, len(snapshot.Schemas))
	schemaProgress := progressbar.NewOptions(len(snapshot.Schemas), progressbar.OptionSetDescription(fmt.Sprintf("  + Restoring all %d schemas...", len(snapshot.Schemas))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for schemaName, schemaHash := range snapshot.Schemas {
		schema, err := backupReader.ReadSchema(schemaHash)
		if err != nil {
			uc.logger.Errorf("could not read %s schema from backup: %v\n", schemaName, err)
			uc.rollbackRestore(dbWriter)
			return
		}

		if err := dbWriter.WriteSchema(schema); err != nil {
			uc.logger.Errorf("could not restore %s schema: %v\n", schemaName, err)
			uc.rollbackRestore(dbWriter)
			return
		}

		schemas = append(schemas, schema)
		schemaProgress.Add(1)
	}
	fmt.Println("  - All schemas restored successfully")

	fmt.Println("  + Restoring schema rules...")
	for _, schema := range schemas {
		if err := dbWriter.WriteSchemaRules(schema); err != nil {
			uc.logger.Errorf("could not restore %s schema rules: %v\n", schema.GetName(), err)
			uc.rollbackRestore(dbWriter)
			return
		}
	}
	fmt.Println("  - All schema rules restored successfully")

	if err := dbWriter.CommitTransaction(); err != nil {
		uc.logger.Errorf("could to commit restored DB: %v\n", err)
		uc.rollbackRestore(dbWriter)
		return
	}
	fmt.Println("Restored DB successfully")
}

func (uc *RestoreUsecases) rollbackRestore(dbWriter services.DatabaseWriter) {
	fmt.Println("Process failed. Aborting operation...")
	fmt.Println("  - Rollback DB to previous state...")

	if err := dbWriter.RollbackTransaction(); err != nil {
		uc.logger.Errorf("could not rollback DB to previous state: %v\n", err)
		return
	}

	fmt.Println("  + Rollback completed")
	fmt.Println("Closing app...")
}
