package binary

import (
	"fmt"
	"historydb/src/internal/entities"
	"historydb/src/internal/services"
	"historydb/src/internal/services/backup/base"
	"io/fs"
	"os"
	"path/filepath"
)

type BinaryBackupWriter struct {
	base.BaseBackupWriter
}

func NewBinaryBackupWriter(backupPath string) *BinaryBackupWriter {
	return &BinaryBackupWriter{BaseBackupWriter: base.BaseBackupWriter{BackupPath: backupPath}}
}

func (writer *BinaryBackupWriter) CommitSnapshot(metadata *entities.BackupMetadata) error {
	if writer.TxSnapshot == nil {
		return services.ErrBackupTransactionNotFound
	}

	content := writer.TxSnapshot.EncodeToBytes()
	pathToFile := filepath.Join(writer.BackupPath, writer.TxSnapshot.SnapshotId, "snapshots", fmt.Sprintf("%s.hdb", writer.TxSnapshot.SnapshotId))
	if err := os.MkdirAll(filepath.Dir(pathToFile), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(pathToFile, content, 0644); err != nil {
		return err
	}

	newSnapshots := append([]entities.BackupMetadataSnapshot{{Timestamp: writer.TxSnapshot.Timestamp, SnapshotId: writer.TxSnapshot.SnapshotId}}, metadata.Snapshots...)
	metadata.Snapshots = newSnapshots

	content = metadata.EncodeToBytes()
	pathToFile = filepath.Join(writer.BackupPath, "metadata.hdb")
	if err := os.WriteFile(pathToFile, content, 0644); err != nil {
		return err
	}

	transactionDir := filepath.Join(writer.BackupPath, writer.TxSnapshot.SnapshotId)
	if err := filepath.WalkDir(transactionDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(transactionDir, path)
		if err != nil {
			return err
		}

		dst := filepath.Join(writer.BackupPath, rel)
		err = os.Rename(path, dst)
		return err
	}); err != nil {
		return err
	}

	if err := os.RemoveAll(transactionDir); err != nil {
		return fmt.Errorf("failed to remove backup transaction dir %s: %w", transactionDir, err)
	}

	writer.TxSnapshot = nil
	return nil
}

func (writer *BinaryBackupWriter) SaveSchemaDependency(dependency entities.SchemaDependency) error {
	if writer.TxSnapshot == nil {
		return services.ErrBackupTransactionNotFound
	}

	content := dependency.EncodeToBytes()
	hash := dependency.Hash()

	pathToFile := filepath.Join(writer.BackupPath, writer.TxSnapshot.SnapshotId, "schemas", "dependencies", fmt.Sprintf("%s.hdb", hash))
	if err := os.MkdirAll(filepath.Dir(pathToFile), 0755); err != nil {
		return err
	}

	return os.WriteFile(pathToFile, content, 0644)
}

func (writer *BinaryBackupWriter) SaveSchemaDependencyDiff(diff entities.SchemaDependencyDiff) error {
	if writer.TxSnapshot == nil {
		return services.ErrBackupTransactionNotFound
	}

	content := diff.EncodeToBytes()
	hash := diff.Hash()

	pathToFile := filepath.Join(writer.BackupPath, writer.TxSnapshot.SnapshotId, "schemas", "dependencies", "diffs", fmt.Sprintf("%s.hdb", hash))
	if err := os.MkdirAll(filepath.Dir(pathToFile), 0755); err != nil {
		return err
	}

	return os.WriteFile(pathToFile, content, 0644)
}
