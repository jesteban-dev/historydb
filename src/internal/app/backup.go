package app

import (
	"flag"
	"fmt"
	"historydb/src/internal/usecases"
	"log/slog"
	"net/url"
)

func BackupApp(args []string, logger *slog.Logger) {
	if len(args) < 1 {
		printBackupHelp()
		return
	}

	backupFlags := flag.NewFlagSet("backup", flag.ExitOnError)
	backupFlags.Usage = printBackupHelp

	action := args[0]
	connString := backupFlags.String("connString", "", "Database connection string")
	basePath := backupFlags.String("path", "", "Path where the backup directory is located, or where it will be created")
	backupFlags.Parse(args[1:])

	engine, err := checkBackupArgsAndObtainEngine(action, *connString, *basePath)
	if err != nil {
		return
	}

	db, err := openDBConnection(engine, *connString)
	if err != nil {
		return
	}
	defer db.Close()

	dbFactory := createDatabaseFactory(engine, db)
	backupFactory := createBackupFactory(*basePath)

	backupUsecases := usecases.NewBackupUsecases(dbFactory, backupFactory)

	switch action {
	case "create":
		backupUsecases.CreateBackup()
	}
}

func checkBackupArgsAndObtainEngine(action, connString, path string) (string, error) {
	if _, ok := supportedActions[action]; !ok {
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
	fmt.Println("Options:")
	fmt.Println("  --connString \tDatabase connection string")
	fmt.Println("  --path \tPath where the backup directory is located, or where it will be created")
}
