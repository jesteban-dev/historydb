package handlers

import "historydb/src/internal/usecases"

type BackupHandler struct {
	backupUc usecases.BackupUsecases
}

func NewBackupHandler(backupUc usecases.BackupUsecases) *BackupHandler {
	return &BackupHandler{backupUc}
}

func (handler *BackupHandler) CreateBackup() {
	snapshot := handler.backupUc.CreateSnapshot(true)
	if snapshot == nil {
		return
	}

	if ok := handler.backupUc.BackupSchemaDependencies(snapshot); !ok {
		return
	}

	schemas, ok := handler.backupUc.BackupSchemas(snapshot)
	if !ok {
		return
	}

	for _, schema := range schemas {
		if ok := handler.backupUc.BackupSchemaData(snapshot, schema); !ok {
			return
		}
	}

	handler.backupUc.CommitSnapshot(nil, snapshot, true)
}

func (handler *BackupHandler) SnapshotBackup() {
	newSnapshot := handler.backupUc.CreateSnapshot(true)
	backupMetadata := handler.backupUc.GetBackupMetadata()
	if backupMetadata == nil {
		return
	}
	lastSnapshot := &backupMetadata.Snapshots[len(backupMetadata.Snapshots)-1]

	if ok := handler.backupUc.SnapshotSchemaDependencies(lastSnapshot, newSnapshot); !ok {
		return
	}

	if ok := handler.backupUc.SnapshotSchemas(lastSnapshot, newSnapshot); !ok {
		return
	}

	handler.backupUc.CommitSnapshot(backupMetadata, newSnapshot, false)
}
