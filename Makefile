# App name and version
APP_NAME = historydb
APP_VERSION = $(shell git describe --tags --always)

# Default installation directory
SRCDIR = src/cmd
BINDIR = /usr/local/bin

# Installs the app into system or user bin
install: build
	@echo "Building $(APP_NAME)..."
	@go build -ldflags="-s -w -X main.version=$(APP_VERSION)" -o $(APP_NAME) ./$(SRCDIR)
	@echo "  Built successfully"

	@echo "Installing $(APP_NAME) to $(BINDIR)..."
	@install -Dm755 $(APP_NAME) $(BINDIR)/$(APP_NAME)
	@echo "  Installed successfully"
	
	@echo "Cleaning up installation files..."
	@rm -f $(APP_NAME)
	@echo "  Cleaned successfully"

# Uninstall the app from system or user bin
uninstall:
	@echo "Uninstalling $(APP_NAME) from $(BINDIR)..."
	@rm -f $(BINDIR)/$(APP_NAME)
	@echo "Uninstalled successfully."

.PHONY: build install clean uninstall