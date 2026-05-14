#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Installing fdbcli-next...${NC}"

# Check if curl is installed
if ! command -v curl &> /dev/null; then
    echo -e "${RED}Error: curl is not installed${NC}"
    exit 1
fi

# Detect OS
OS=$(uname -s)

case "$OS" in
    Linux*)
        INSTALL_DIR="/usr/local/bin"
        ;;
    Darwin*)
        INSTALL_DIR="/usr/local/bin"
        ;;
    *)
        echo -e "${RED}Error: Unsupported operating system: $OS${NC}"
        exit 1
        ;;
esac

# The binary searches for libfdb_c.so in /usr/lib64
FDB_LIB_TARGET="/usr/lib64"

echo "Detected OS: $OS"

# Create temporary directory for download
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

cd "$TEMP_DIR"

# Download the latest release
DOWNLOAD_URL="https://github.com/Pranav2612000/fdbcli-next/releases/latest/download/fdbcli-next"
echo "Downloading from: $DOWNLOAD_URL"

if ! curl -L -o fdbcli-next "$DOWNLOAD_URL"; then
    echo -e "${RED}Error: Failed to download fdbcli-next${NC}"
    exit 1
fi

# Make the binary executable
chmod +x fdbcli-next

# Check if installation directory exists and is writable
if [ ! -d "$INSTALL_DIR" ]; then
    echo -e "${YELLOW}Creating $INSTALL_DIR...${NC}"
    mkdir -p "$INSTALL_DIR"
fi

# Install the binary
echo "Installing to $INSTALL_DIR..."
if [ -w "$INSTALL_DIR" ]; then
    cp fdbcli-next "$INSTALL_DIR/"
else
    echo -e "${YELLOW}Requires sudo to install to $INSTALL_DIR${NC}"
    sudo cp fdbcli-next "$INSTALL_DIR/"
fi

# Verify installation
if command -v fdbcli-next &> /dev/null; then
    echo -e "${GREEN}Successfully installed fdbcli-next${NC}"
    echo "Binary location: $(which fdbcli-next)"
    echo "Version: $(fdbcli-next --version 2>/dev/null || echo 'N/A')"
else
    echo -e "${RED}Installation verification failed${NC}"
    exit 1
fi

# Check for and setup FDB C library
echo ""
echo "Setting up FoundationDB C library..."

FDB_LIB_FOUND=false

# First, check if library already exists in target location
if [ -f "$FDB_LIB_TARGET/libfdb_c.so" ]; then
    echo -e "${GREEN}Found FDB library: $FDB_LIB_TARGET/libfdb_c.so${NC}"
    FDB_LIB_FOUND=true
fi

# If not found, look for it in standard locations and copy to /usr/lib64
if [ "$FDB_LIB_FOUND" = false ]; then
    echo "Searching for FoundationDB libraries..."

    FOUND_LIB=""

    # Search in common FDB installation paths
    for search_path in "/usr/lib/fdb/multiversion" "/usr/lib" "/usr/lib64" "/opt/homebrew/lib"; do
        if [ -d "$search_path" ]; then
            LATEST_LIB=$(ls -1 "$search_path"/libfdb_c*.so 2>/dev/null | sort -V | tail -1)
            if [ -n "$LATEST_LIB" ]; then
                FOUND_LIB="$LATEST_LIB"
                break
            fi
        fi
    done

    if [ -n "$FOUND_LIB" ]; then
        echo "Found library: $FOUND_LIB"
        echo "Copying to /usr/lib64/libfdb_c.so..."

        # Ensure /usr/lib64 directory exists
        if [ ! -d "/usr/lib64" ]; then
            if [ -w "/usr" ]; then
                mkdir -p /usr/lib64
            else
                sudo mkdir -p /usr/lib64
            fi
        fi

        # Copy the library to /usr/lib64
        if [ -w "/usr/lib64" ]; then
            cp "$FOUND_LIB" "/usr/lib64/libfdb_c.so"
            echo -e "${GREEN}Copied library to /usr/lib64/libfdb_c.so${NC}"
            FDB_LIB_FOUND=true
        else
            echo -e "${YELLOW}Need sudo to copy library to /usr/lib64${NC}"
            if sudo cp "$FOUND_LIB" "/usr/lib64/libfdb_c.so"; then
                echo -e "${GREEN}Copied library to /usr/lib64/libfdb_c.so${NC}"
                FDB_LIB_FOUND=true
            else
                echo -e "${RED}Failed to copy library${NC}"
            fi
        fi
    fi
fi

if [ "$FDB_LIB_FOUND" = false ]; then
    echo -e "${RED}Error: FoundationDB C library not found${NC}"
    echo ""
    echo "To install FoundationDB:"
    echo "  On Ubuntu/Debian:"
    echo "    curl https://foundationdb.org/downloads/ubuntu/installers.html"
    echo "    sudo apt-get install foundationdb"
    echo ""
    echo "  On other Linux distributions:"
    echo "    Visit: https://foundationdb.org/download/"
    echo ""
    echo "  On macOS:"
    echo "    brew install foundationdb"
    echo ""
    exit 1
fi

echo ""
echo -e "${GREEN}Installation complete!${NC}"
echo ""
echo "Next steps:"
echo "  1. Ensure FoundationDB C library is accessible (see above if not found)"
echo "  2. Set FDB cluster file: export FDBCLI_DB_PATH=\"/path/to/fdb.cluster\""
echo "  3. Run: fdbcli-next repl"
echo ""
echo "For more information, visit: https://github.com/Pranav2612000/fdbcli-next"
