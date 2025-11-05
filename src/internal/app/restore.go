package app

import (
	"errors"
	"flag"
	"fmt"
	"historydb/src/internal/handlers"
	"historydb/src/internal/usecases"
	"historydb/src/internal/utils/pointers"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

func RestoreApp(args []string) {
	restoreFlags := flag.NewFlagSet("restore", flag.ExitOnError)
	restoreFlags.Usage = printRestoreHelp

	connString := restoreFlags.String("connString", "", "Database connection string where to restore all the data")
	basePath := restoreFlags.String("path", "", "Path where the backup is located")
	snapshotArg := restoreFlags.String("from", "", "Snapshot ID or Timestamp from where to restore the database")

	if err := restoreFlags.Parse(args); err != nil {
		return
	}

	snapshot, err := checkSnapshot(*snapshotArg)
	if err != nil {
		panic(err)
	}

	engine, err := checkRestoreArgsAndObtainEngine(*connString, *basePath)
	if err != nil {
		if errors.Is(err, ErrUnsuportedAction) || errors.Is(err, ErrArgumentNotProvided) {
			return
		}
		panic(err)
	}

	db, err := openDBConnection(engine, *connString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if _, err := os.Stat(*basePath); err != nil {
		fmt.Println("The specified path does not seem to contain a backup")
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

	restoreUsecases := usecases.NewRestoreUsecasesImpl(dbFactory, backupFactory, logger)

	restoreHandler := handlers.NewRestoreHandler(restoreUsecases)
	restoreHandler.RestoreDatabase(snapshot)
}

func checkSnapshot(snapshot string) (*string, error) {
	if snapshot == "" {
		return nil, nil
	}

	_, err := uuid.Parse(snapshot)
	if err != nil {
		if _, err := time.Parse(time.RFC3339, snapshot); err != nil {
			fmt.Println("--from argument needs to be a UUID or RFC3339 timestamp format")
			return nil, fmt.Errorf("invalid --from")
		}
	}

	return pointers.Ptr(snapshot), nil
}

func checkRestoreArgsAndObtainEngine(connString, path string) (string, error) {
	if connString == "" {
		fmt.Printf("It is required to provide the argument --connString\n")
		return "", ErrArgumentNotProvided
	}
	if path == "" {
		fmt.Print("It is required to provide the argument --path\n")
		return "", ErrArgumentNotProvided
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

func printRestoreHelp() {
	fmt.Println("Usage: historydb restroe [options]")
	fmt.Println("Options:")
	fmt.Println("  --connString \tDatabase connection string where to restore all the data")
	fmt.Println("  --path \tPath where the backup is located")
	fmt.Println("  --from \tSnapshot ID or Timestamp from where to restore the database")
}
