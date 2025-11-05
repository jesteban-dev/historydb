package main

import (
	"flag"
	"fmt"
	"historydb/src/internal/app"
	"os"
)

var version string

func main() {
	fmt.Println("historydb version:", version)
	if len(os.Args) < 2 {
		printRootHelp()
		return
	}

	flag.Usage = func() {
		fmt.Println("Usage: historydb [mode] [action] [options]")
	}

	switch os.Args[1] {
	case "backup":
		app.BackupApp(os.Args[2:])
	case "restore":
		app.RestoreApp(os.Args[2:])
	case "log":
		app.LogApp(os.Args[2:])
	default:
		printRootHelp()
	}
}

func printRootHelp() {
	fmt.Println("Expected mode command. Usage: historydb [mode] [args...]")
	fmt.Println("Supported modes:")
	fmt.Println("  - backup: \tIt creates or updates backups from a database.")
	fmt.Println("  - restore: \tIt restores your database from a backup.")
}
