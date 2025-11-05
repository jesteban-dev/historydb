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

	if ok := handler.restoreUc.BeginDatabaseRestore(); !ok {
		return
	}

	if ok := handler.restoreUc.RestoreSchemaDependencies(snapshot); !ok {
		handler.restoreUc.RollbackDatabaseRestore()
		return
	}

	schemas := handler.restoreUc.RestoreSchemas(snapshot)
	if schemas == nil {
		handler.restoreUc.RollbackDatabaseRestore()
		return
	}

	for _, schema := range schemas {
		if ok := handler.restoreUc.RestoreSchemaRecords(snapshot, schema); !ok {
			handler.restoreUc.RollbackDatabaseRestore()
			return
		}
	}

	if ok := handler.restoreUc.CommitDatabaseRestore(); !ok {
		handler.restoreUc.RollbackDatabaseRestore()
	}
}
