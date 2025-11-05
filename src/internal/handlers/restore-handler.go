package handlers

import "historydb/src/internal/usecases"

type RestoreHanlder struct {
	restoreUc usecases.RestoreUsecases
}

func NewRestoreHandler(restoreUc usecases.RestoreUsecases) *RestoreHanlder {
	return &RestoreHanlder{restoreUc}
}

func (handler *RestoreHanlder) RestoreDatabase(snapshotId *string) {
	snapshot := handler.restoreUc.GetBackupSnapshot(snapshotId)
	if snapshot == nil {
		return
	}

	if ok := handler.restoreUc.StartDatabaseRestore(); !ok {
		return
	}

	if ok := handler.restoreUc.RestoreSchemaDependencies(snapshot); !ok {
		return
	}

	if ok := handler.restoreUc.RestoreSchemas(snapshot); !ok {
		return
	}

	handler.restoreUc.EndDatabaseRestore()
}
