package app

import (
	"flag"
	"fmt"
	"historydb/src/internal/handlers"
	"historydb/src/internal/usecases"
	"net/url"
	"os"
	"path"

	"github.com/sirupsen/logrus"
)

// BackupApp is the main execution for backup mode in the app
func BackupApp(args []string) {
	if len(args) < 1 {
		printBackupHelp()
		return
	}

	backupFlags := flag.NewFlagSet("backup", flag.ExitOnError)
	backupFlags.Usage = printBackupHelp

	action := args[0]
	connString := backupFlags.String("connString", "", "Database connection string")
	basePath := backupFlags.String("path", "", "Path where the backup directory is located, or where it will be created")
	message := backupFlags.String("message", "", "Optional message which will be saved in the snapshot")
	backupFlags.Parse(args[1:])

	engine, err := checkBackupArgsAndObtainEngine(action, *connString, *basePath)
	if err != nil {
		panic(err)
	}

	db, err := openDBConnection(engine, *connString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := os.MkdirAll(*basePath, 0755); err != nil {
		return
	}
	loggerFile, err := os.OpenFile(path.Join(*basePath, "backup.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}

	logger := &logrus.Logger{
		Out:       loggerFile,
		Level:     logrus.InfoLevel,
		Formatter: &logrus.TextFormatter{FullTimestamp: true},
	}
	logrus.SetLevel(logrus.InfoLevel)

	dbFactory := createDatabaseFactory(engine, db)
	backupFactory := createBackupFactory(*basePath)

	backupUsecases := usecases.NewBackupUsecasesImpl(dbFactory, backupFactory, logger)

	backupHandler := handlers.NewBackupHandler(backupUsecases)

	switch action {
	case "create":
		backupHandler.CreateBackup(*message)
	case "snapshot":
		backupHandler.SnapshotBackup(*message)
	}
}

func checkBackupArgsAndObtainEngine(action, connString, path string) (string, error) {
	if _, ok := supportedBackupActions[action]; !ok {
		fmt.Printf("The action '%s' is not supported in the backup app.\n", action)
		return "", fmt.Errorf("unsupported action provided")
	}

	if connString == "" {
		fmt.Printf("It is required to provide the argument --connString\n")
		return "", fmt.Errorf("no --connString provided")
	}
	if path == "" {
		fmt.Print("It is required to provide the argument --path\n")
		return "", fmt.Errorf("no --path provided")
	}

	parsedCDN, err := url.Parse(connString)
	if err != nil || parsedCDN.Scheme == "" {
		fmt.Printf("Invalid database connection string '%s'\n", connString)
		return "", fmt.Errorf("invalid dsn")
	}
	engine, ok := supportedEngines[parsedCDN.Scheme]
	if !ok {
		fmt.Printf("The engine '%s' is not supported in the backup app.\n", parsedCDN.Scheme)
		return "", fmt.Errorf("unsupported db engine")
	}

	return engine, nil
}

func printBackupHelp() {
	fmt.Println("Usage: historydb backup [action] [options]")
	fmt.Println("Actions:")
	fmt.Println("  create \tIt creates a new backup from a database")
	fmt.Println("  snapshot \tIt snapshots the current state of the database into the already created backup")
	fmt.Println("Options:")
	fmt.Println("  --connString \tDatabase connection string from where to back-up the data")
	fmt.Println("  --path \tPath where the backup is located, or where it will be created")
	fmt.Println("  --message \tOptional message which will be saved in the snapshot")
}
