# Changelog

All notable changes to this project will be documented in this file.

## [v1.0.1] - 2026-01-08
### Fixed
- Support for BigInt type in PostgreSQL databases.
### Changed
- PSQLDatabaseReader unit tests to include expected data from an external json file, and include support for test with init script files.
- BinaryBackupReader unit tests to inclide expected data from an external json file, and included a default backup directory for testing.
- Cleaned some unused keyworks in Makefile.