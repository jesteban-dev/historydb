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

type RestoreUsecasesImpl struct {
	dbFactory     services.DatabaseFactory
	backupFactory services.BackupFactory
	logger        *logrus.Logger
}

func NewRestoreUsecasesImpl(dbFactory services.DatabaseFactory, backupFactory services.BackupFactory, logger *logrus.Logger) *RestoreUsecasesImpl {
	return &RestoreUsecasesImpl{dbFactory, backupFactory, logger}
}

// Resturns the specified snapshot selected by the user to restore the DB, if the snapshot is nil it will use the last one
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
	backupMetadata, err := backupReader.ReadBackupMetadata()
	if err != nil {
		fmt.Println("The specified path does not contain a backup")
		return nil
	}
	if !uc.dbFactory.CheckBackupDB(backupMetadata.Database) {
		fmt.Println("The database and backup engines does not match")
		return nil
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

	return &snapshot
}

// Starts the DB transaction for restoring the database
func (uc *RestoreUsecasesImpl) StartDatabaseRestore() bool {
	dbWriter := uc.dbFactory.CreateWriter()

	if err := dbWriter.BeginTransaction(); err != nil {
		uc.logger.Errorf("could not begin DB transaction: %v\n", err)
		return false
	}

	return true
}

// Restore the schema dependencies from the backup to the DB
func (uc *RestoreUsecasesImpl) RestoreSchemaDependencies(snapshot *entities.BackupSnapshot) bool {
	backupReader := uc.backupFactory.CreateReader()
	dbWriter := uc.dbFactory.CreateWriter()

	schemaDependenciesProgress := progressbar.NewOptions(len(snapshot.SchemaDependencies), progressbar.OptionSetDescription(fmt.Sprintf("  + Restoring all %d schema dependencies...", len(snapshot.SchemaDependencies))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for dependencyName, dependencyHash := range snapshot.SchemaDependencies {
		dependency, err := backupReader.ReadSchemaDependency(dependencyHash)
		if err != nil {
			uc.logger.Errorf("could not read %s schema dependency from backup: %v\n", dependencyName, err)
			uc.rollbackRestore(dbWriter)
			return false
		}

		if err := dbWriter.WriteSchemaDependency(dependency); err != nil {
			uc.logger.Errorf("could not restore %s schema dependency: %v\n", dependencyName, err)
			uc.rollbackRestore(dbWriter)
			return false
		}

		schemaDependenciesProgress.Add(1)
	}
	fmt.Println("  - All schema dependencies restored successfully")
	return true
}

// Restore the schemas from the backup to the DB
func (uc *RestoreUsecasesImpl) RestoreSchemas(snapshot *entities.BackupSnapshot) bool {
	backupReader := uc.backupFactory.CreateReader()
	dbWriter := uc.dbFactory.CreateWriter()

	schemas := make([]entities.Schema, 0, len(snapshot.Schemas))
	schemaProgress := progressbar.NewOptions(len(snapshot.Schemas), progressbar.OptionSetDescription(fmt.Sprintf("  + Restoring all %d schemas...", len(snapshot.Schemas))), progressbar.OptionSetWidth(30), progressbar.OptionSetWriter(os.Stdout), progressbar.OptionSetRenderBlankState(true))
	for schemaName, schemaHash := range snapshot.Schemas {
		schema, err := backupReader.ReadSchema(schemaHash)
		if err != nil {
			uc.logger.Errorf("could not read %s schema from backup: %v\n", schemaName, err)
			uc.rollbackRestore(dbWriter)
			return false
		}

		if err := dbWriter.WriteSchema(schema); err != nil {
			uc.logger.Errorf("could not restore %s schema: %v\n", schemaName, err)
			uc.rollbackRestore(dbWriter)
			return false
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
			return false
		}
	}
	fmt.Println("  - All schema rules restored successfully")

	return true
}

// Ends the DB transaction for restoring the backup
func (uc *RestoreUsecasesImpl) EndDatabaseRestore() bool {
	dbWriter := uc.dbFactory.CreateWriter()

	if err := dbWriter.CommitTransaction(); err != nil {
		uc.logger.Errorf("could to commit restored DB: %v\n", err)
		uc.rollbackRestore(dbWriter)
		return false
	}
	fmt.Println("Restored DB successfully")
	return true
}

// Rollbacks the DB to the last stable state if the restore fails
func (uc *RestoreUsecasesImpl) rollbackRestore(dbWriter services.DatabaseWriter) {
	fmt.Println("Process failed. Aborting operation...")
	fmt.Println("  - Rollback DB to previous state...")

	if err := dbWriter.RollbackTransaction(); err != nil {
		uc.logger.Errorf("could not rollback DB to previous state: %v\n", err)
		return
	}

	fmt.Println("  + Rollback completed")
	fmt.Println("Closing app...")
}
