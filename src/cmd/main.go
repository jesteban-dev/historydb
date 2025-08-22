package main

import (
	"flag"
	"fmt"
	"historydb/src/internal/app"
	"historydb/src/internal/helpers"
	"log/slog"
	"os"
)

func main() {
	logger := slog.New(&helpers.LogFormatter{})

	if len(os.Args) < 2 {
		printRootHelp()
		return
	}

	flag.Usage = func() {
		fmt.Println("Usage: historydb [mode] [action] [options]")
	}

	switch os.Args[1] {
	case "backup":
		app.BackupApp(os.Args[2:], logger)
	default:
		printRootHelp()
	}
}

func printRootHelp() {
	fmt.Println("Expected mode command. Usage: historydb [mode] [args...]")
	fmt.Println("Supported modes:")
	fmt.Println("  - backup: \tIt creates or updates backups from a database.")
	fmt.Println("  - restore: \tIt restores your database from a backup.")
	fmt.Println("  - migrate: \tIt migrate an already existent backup from an engine to a different one.")
}
