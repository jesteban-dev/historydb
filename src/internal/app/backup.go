package app

import (
	"flag"
	"fmt"
	"historydb/src/internal/usecases"
	"log/slog"
	"net"
	"net/url"
	"strconv"
)

// BackupApp is the main execution for backup mode in the app
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

	dsn, _ := url.Parse(*connString)
	host, port, _ := net.SplitHostPort(dsn.Host)
	var dbPort int
	if port == "" {
		dbPort, _ = strconv.Atoi(dsn.Port())
	} else {
		dbPort, _ = strconv.Atoi(port)
	}

	dbFactory := createDatabaseFactory(engine, db, host, dbPort, dsn.Path[1:])
	backupFactory := createBackupFactory(*basePath)

	backupUsecases := usecases.NewBackupUsecases(dbFactory, backupFactory, logger)

	switch action {
	case "create":
		backupUsecases.CreateBackup()
	case "snapshot":
		backupUsecases.SnapshotBackup()
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
}
