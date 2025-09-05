package app

import (
	"flag"
	"fmt"
	"historydb/src/internal/helpers"
	"historydb/src/internal/usecases"
	"net"
	"net/url"
	"os"
	"strconv"
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

	restoreFlags.Parse(args)

	snapshot, err := checkSnapshot(*snapshotArg)
	if err != nil {
		return
	}

	engine, err := checkRestoreArgsAndObtainEngine(*connString, *basePath)
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

	logger := &logrus.Logger{
		Out:       os.Stdout,
		Level:     logrus.InfoLevel,
		Formatter: &logrus.TextFormatter{FullTimestamp: true},
	}

	dbFactory := createDatabaseFactory(engine, db, host, dbPort, dsn.Path[1:])
	backupFactory := createBackupFactory(*basePath)

	restoreUsecases := usecases.NewRestoreUsecases(dbFactory, backupFactory, logger)
	restoreUsecases.RestoreDatabase(snapshot)
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

	return helpers.Pointer(snapshot), nil
}

func checkRestoreArgsAndObtainEngine(connString, path string) (string, error) {
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

func printRestoreHelp() {
	fmt.Println("Usage: historydb restroe [options]")
	fmt.Println("Options:")
	fmt.Println("  --connString \tDatabase connection string where to restore all the data")
	fmt.Println("  --path \tPath where the backup is located")
	fmt.Println("  --from \tSnapshot ID or Timestamp from where to restore the database")
}
