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
		handler.backupUc.RollbackSnapshot(true)
		return
	}

	if ok := handler.backupUc.BackupSchemaDependencies(snapshot); !ok {
		handler.backupUc.RollbackSnapshot(true)
		return
	}

	schemas := handler.backupUc.BackupSchemas(snapshot)
	if schemas == nil {
		handler.backupUc.RollbackSnapshot(true)
		return
	}

	if ok := handler.backupUc.CommitSnapshot(nil, snapshot); !ok {
		handler.backupUc.RollbackSnapshot(true)
	}
}

func (handler *BackupHandler) SnapshotBackup() {
	backupMetadata := handler.backupUc.GetBackupMetadata()
	if backupMetadata == nil {
		return
	}
	lastSnapshot := handler.backupUc.GetSnapshot(backupMetadata.Snapshots[len(backupMetadata.Snapshots)-1].SnapshotId)
	if lastSnapshot == nil {
		return
	}

	newSnapshot := handler.backupUc.CreateSnapshot(false)
	if newSnapshot == nil {
		handler.backupUc.RollbackSnapshot(false)
		return
	}

	if ok := handler.backupUc.SnapshotSchemaDependencies(lastSnapshot, newSnapshot); !ok {
		handler.backupUc.RollbackSnapshot(false)
		return
	}

	schemas := handler.backupUc.SnapshotSchemas(lastSnapshot, newSnapshot)
	if schemas == nil {
		handler.backupUc.RollbackSnapshot(false)
		return
	}

	if ok := handler.backupUc.CommitSnapshot(backupMetadata, newSnapshot); !ok {
		handler.backupUc.RollbackSnapshot(false)
	}
}
