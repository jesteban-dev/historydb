package usecases

import (
	"errors"
	"fmt"
	"historydb/src/internal/services"
	backup_services "historydb/src/internal/services/backup"
	"os"
	"os/exec"
	"time"

	"github.com/sirupsen/logrus"
)

type LogUsecasesImpl struct {
	backupFactory backup_services.BackupFactory
	logger        *logrus.Logger
}

func NewLogUsecasesImpl(backupFactory backup_services.BackupFactory, logger *logrus.Logger) *LogUsecasesImpl {
	return &LogUsecasesImpl{backupFactory, logger}
}

func (uc *LogUsecasesImpl) ListSnapshots() {
	backupReader := uc.backupFactory.CreateReader()

	if ok := backupReader.CheckBackupExists(); !ok {
		fmt.Println("The specified backup path does not exist.")
		return
	}

	backupMetadata, err := backupReader.GetBackupMetadata()
	if err != nil {
		if errors.Is(err, services.ErrBackupCorruptedFile) {
			fmt.Println("The specified backup is corrupted.")
		}

		uc.logger.Errorf("could not retrieve backup metadata: %v", err)
		return
	}

	cmd := exec.Command("less", "-R")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	writer, err := cmd.StdinPipe()
	if err != nil {
		uc.logger.Errorf("could not open log writer: %v", err)
		return
	}
	cmd.Start()

	// ANSI color codes
	red := "\033[31m"
	yellow := "\033[33m"
	reset := "\033[0m"

	fmt.Fprintf(writer, red+"Database engine: %s\n"+reset, backupMetadata.DatabaseEngine)
	fmt.Fprintf(writer, red+"Total snapshots: %d\n"+reset, len(backupMetadata.Snapshots))
	for i := len(backupMetadata.Snapshots) - 1; i >= 0; i-- {
		fmt.Fprintf(writer, "*  "+yellow+"Snapshot %s taken at %s\n"+reset, backupMetadata.Snapshots[i].SnapshotId, backupMetadata.Snapshots[i].Timestamp.Format(time.RFC3339))
		fmt.Fprintf(writer, "*    Message: %s\n", backupMetadata.Snapshots[i].Message)

		if i != 0 {
			fmt.Fprintf(writer, "▲\n|\n|\n")
		}
	}

	writer.Close()
	cmd.Wait()
}
