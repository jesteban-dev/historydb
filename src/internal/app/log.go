package app

import (
	"flag"
	"fmt"
	"historydb/src/internal/handlers"
	"historydb/src/internal/usecases"
	"os"
	"path"

	"github.com/sirupsen/logrus"
)

func LogApp(args []string) {
	if len(args) < 1 {
		printLogHelp()
		return
	}

	logFlags := flag.NewFlagSet("log", flag.ExitOnError)
	logFlags.Usage = printLogHelp

	backupPath := logFlags.String("path", "", "Path where the backup is located")
	logFlags.Parse(args[:])

	loggerFile, err := os.OpenFile(path.Join(*backupPath, "backup.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("There is no backup located in the specified path")
		return
	}

	logger := &logrus.Logger{
		Out:       loggerFile,
		Level:     logrus.InfoLevel,
		Formatter: &logrus.TextFormatter{FullTimestamp: true},
	}
	logrus.SetLevel(logrus.InfoLevel)

	backupFactory := createBackupFactory(*backupPath)

	logUsecases := usecases.NewLogUsecasesImpl(backupFactory, logger)

	logHandler := handlers.NewLogHandler(logUsecases)

	logHandler.ListSnapshots()
}

func printLogHelp() {
	fmt.Println("Usage: historydb log [options]")
	fmt.Println("Options:")
	fmt.Println("  --path \tPath where the backup is located")
}
