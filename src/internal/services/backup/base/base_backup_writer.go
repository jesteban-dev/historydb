package base

import (
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	"os"
	"path/filepath"
)

type BaseBackupWriter struct {
	BackupPath string
	TxSnapshot *entities.BackupSnapshot
}

func (writer *BaseBackupWriter) CreateBackupStructure() error {
	_, err := os.Stat(writer.BackupPath)
	if err == nil {
		return services.ErrBackupDirExists
	}

	if err := os.Mkdir(writer.BackupPath, 0755); err != nil {
		return err
	}

	if err := os.Mkdir(filepath.Join(writer.BackupPath, "snapshots"), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(writer.BackupPath, "schemas"), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(writer.BackupPath, "schemas", "diffs"), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(writer.BackupPath, "schemas", "dependencies"), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(writer.BackupPath, "schemas", "dependencies", "diffs"), 0755); err != nil {
		return err
	}

	if err := os.Mkdir(filepath.Join(writer.BackupPath, "data"), 0755); err != nil {
		return err
	}

	return os.Mkdir(filepath.Join(writer.BackupPath, "data", "diffs"), 0755)
}

func (writer *BaseBackupWriter) DeleteBackupStructure() error {
	return os.RemoveAll(writer.BackupPath)
}

func (writer *BaseBackupWriter) BeginSnapshot(snapshot *entities.BackupSnapshot) error {
	if writer.TxSnapshot != nil {
		return services.ErrBackupTransactionInProgress
	}

	writer.TxSnapshot = snapshot
	return nil
}

func (writer *BaseBackupWriter) RollbackSnapshot() error {
	if writer.TxSnapshot == nil {
		return services.ErrBackupTransactionNotFound
	}

	transactionDir := filepath.Join(writer.BackupPath, writer.TxSnapshot.SnapshotId)

	if err := os.RemoveAll(transactionDir); err != nil {
		return err
	}

	writer.TxSnapshot = nil
	return nil
}
