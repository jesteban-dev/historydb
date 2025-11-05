# HistoryDB
![Version](https://img.shields.io/badge/version-1.0-blue)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)
![Go](https://img.shields.io/badge/Go-1.23.2-blue?logo=go&logoColor=white)

**HistoryDB** is a lightweight backup system for databases which combines the reliability of versioned backups with the efficiency of incremental storage. It allows you to:
- Take snapshots of your database at specific points in time.
- Recover any snapshot using the snapshot id or timestamp, giving you the option to recover different snapshots in time.
- Save storage space by only storing incremental changes instead of full backups every time.

> ⚠️ **Warning:** Currently the system only works for **PostgreSQL** databases. In the future, other DB systems will be included.

### Features
- 🕒 Point-in-time recovery
- 💾 Incremental (diff-based) backups
- ⚡ Lightweight and fast

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
    - [Creating a Backup](#creating-a-backup)
    - [Taking a Diff Snapshot](#taking-a-diff-snapshot)
    - [Restoring your Database](#restoring-a-database)
    - [Viewing Snapshot History](#viewing-snapshot-history)
- [License](#license)


## Installation

**Requirements:**  
- Go 1.23 or higher  
- Make

**Steps:**

> 💡 **Tip:** You can change the installation location modifying the **BINDIR** variable into the Makefile.

> ⚠️ **Note:** The default installation path (`/usr/local/bin`) requires elevated permissions.

```bash
# Clone the repository
git clone https://github.com/username/project.git
cd project

# Build and install the application
sudo make install
```

In case you want to uninstall the app, you can simple run:

```bash
sudo make uninstall
```

## Usage

### Creating a Backup
To start using the system, we need to **create** our first backup of the database:

```bash
historydb backup create \
    --connString "<DATABASE_URL>" \
    --path "<BACKUP_PATH>" \
    --message "our first backup"
```

In which:
- **--connString** is our database connection string. ⚠️ **Warning:** At the moment, it only works with sslmode=disable.
- **--path** is the directory path where our backup will be created. Be sure that this path does not exist in the system since the app will need to create it.
- **--message** is an **optional** parameter which will give our snapshot a message so we have a description of it.

### Taking a diff snapshot
After our first backup is created, we can take snapshots of the database at any moment if you need to save new changes:

```bash
historydb backup snapshot \
    --connString "<DATABASE_URL>" \
    --path "<BACKUP_PATH>" \
    --message "our diff snapshot"
```

- When making an snapshot take into count that the **--path** parameter needs to be the same as the one you used for creating the backup.

### Restoring a database
After having our backup directory with some snapshots, let´s say we lost the data into our database so we want to restore it from the backup. Take in count that for restoring the database you need first to create an **empty database**:

```bash
historydb restore \
    --connString "<DATABASE_URL>" \
    --path "<BACKUP_PATH>" \
    --from "<SNAPSHOT>"
```

In which:
- **--connString** is the database connection string of out empty database where we want to restore the data.
- **--from** is an **optional** parameter in which we can specify the snapshot we want to restore. If we omit the parameter, we will restore the last snapshot taken. For this argument, you can use either the snapshot-id or the snapshot-timestamp provided by the log viewer.

### Viewing Snapshot History

If you want to watch all your snapshots taken into a backup with its IDs, timestamp and the message you provided, you can just use:

```bash
historydb log --path "<BACKUP_PATH>"
```

## Roadmap
- [ ] Add support for MySQL/MariaDB
- [ ] Add support for MongoDB

## License

This project is licensed under the [MIT License](./LICENSE).