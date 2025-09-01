#!/bin/bash

# GA4 Admin Tool Installation Script
# Supports macOS and Linux (x86_64 and ARM64)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# GitHub repository details
REPO="avisekrath/csga4"
BINARY_NAME="ga4admin"

# Detect OS and architecture
detect_platform() {
    local os="$(uname -s)"
    local arch="$(uname -m)"
    
    case "$os" in
        Darwin*)
            OS="darwin"
            ;;
        Linux*)
            OS="linux"
            ;;
        *)
            echo -e "${RED}Error: Unsupported operating system: $os${NC}"
            echo "This installer only supports macOS and Linux"
            exit 1
            ;;
    esac
    
    case "$arch" in
        x86_64|amd64)
            ARCH="amd64"
            ;;
        arm64|aarch64)
            if [ "$OS" = "darwin" ]; then
                ARCH="arm64"
            else
                echo -e "${RED}Error: ARM64 Linux not supported${NC}"
                echo "Supported: macOS (Intel/Apple Silicon), Linux (x86_64 only)"
                exit 1
            fi
            ;;
        *)
            echo -e "${RED}Error: Unsupported architecture: $arch${NC}"
            echo "Supported architectures: x86_64, amd64, arm64 (macOS only)"
            exit 1
            ;;
    esac
    
    PLATFORM="${OS}-${ARCH}"
    BINARY_FILE="${BINARY_NAME}-${PLATFORM}"
}

# Get latest release version from GitHub API
get_latest_version() {
    echo -e "${BLUE}Fetching latest release...${NC}"
    
    if command -v curl > /dev/null 2>&1; then
        VERSION=$(curl -s "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    elif command -v wget > /dev/null 2>&1; then
        VERSION=$(wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    else
        echo -e "${RED}Error: Neither curl nor wget is available${NC}"
        exit 1
    fi
    
    if [ -z "$VERSION" ]; then
        echo -e "${RED}Error: Could not fetch latest version${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Latest version: $VERSION${NC}"
}

# Download binary
download_binary() {
    local download_url="https://github.com/${REPO}/releases/download/${VERSION}/${BINARY_FILE}"
    local temp_file="/tmp/${BINARY_FILE}"
    
    echo -e "${BLUE}Downloading ${BINARY_FILE}...${NC}"
    echo -e "${YELLOW}URL: $download_url${NC}"
    
    if command -v curl > /dev/null 2>&1; then
        curl -L -o "$temp_file" "$download_url"
    elif command -v wget > /dev/null 2>&1; then
        wget -O "$temp_file" "$download_url"
    else
        echo -e "${RED}Error: Neither curl nor wget is available${NC}"
        exit 1
    fi
    
    if [ ! -f "$temp_file" ]; then
        echo -e "${RED}Error: Download failed${NC}"
        exit 1
    fi
    
    # Make executable
    chmod +x "$temp_file"
    echo -e "${GREEN}Downloaded successfully${NC}"
}

# Install binary
install_binary() {
    local temp_file="/tmp/${BINARY_FILE}"
    local install_dir="/usr/local/bin"
    local user_install_dir="$HOME/.local/bin"
    
    # Try to install to /usr/local/bin first (system-wide)
    if [ -w "$install_dir" ] || sudo -n true 2>/dev/null; then
        echo -e "${BLUE}Installing to $install_dir (system-wide)...${NC}"
        if [ -w "$install_dir" ]; then
            mv "$temp_file" "$install_dir/$BINARY_NAME"
        else
            sudo mv "$temp_file" "$install_dir/$BINARY_NAME"
        fi
        INSTALL_PATH="$install_dir/$BINARY_NAME"
    else
        # Fall back to user-local installation
        echo -e "${BLUE}Installing to $user_install_dir (user-only)...${NC}"
        mkdir -p "$user_install_dir"
        mv "$temp_file" "$user_install_dir/$BINARY_NAME"
        INSTALL_PATH="$user_install_dir/$BINARY_NAME"
        
        # Check if user bin is in PATH
        if [[ ":$PATH:" != *":$user_install_dir:"* ]]; then
            echo -e "${YELLOW}Warning: $user_install_dir is not in your PATH${NC}"
            echo -e "${YELLOW}Add this line to your shell profile (~/.bashrc, ~/.zshrc, etc.):${NC}"
            echo -e "${BLUE}export PATH=\"\$PATH:$user_install_dir\"${NC}"
        fi
    fi
    
    echo -e "${GREEN}Installed successfully to: $INSTALL_PATH${NC}"
}

# Verify installation
verify_installation() {
    if command -v "$BINARY_NAME" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Installation successful!${NC}"
        echo -e "${BLUE}Run '${BINARY_NAME} --help' to get started${NC}"
        
        # Show version
        echo -e "${BLUE}Version installed:${NC}"
        "$BINARY_NAME" --version 2>/dev/null || echo "Version information not available"
    else
        echo -e "${YELLOW}Installation completed, but '${BINARY_NAME}' is not in PATH${NC}"
        echo -e "${BLUE}You can run it directly: $INSTALL_PATH${NC}"
    fi
}

# Main installation process
main() {
    echo -e "${GREEN}🚀 GA4 Admin Tool Installer${NC}"
    echo -e "${BLUE}Installing lightweight GA4 CLI tool...${NC}"
    echo
    
    detect_platform
    echo -e "${GREEN}Detected platform: $PLATFORM${NC}"
    
    get_latest_version
    download_binary
    install_binary
    verify_installation
    
    echo
    echo -e "${GREEN}🎉 Installation complete!${NC}"
    echo -e "${BLUE}Next steps:${NC}"
    echo -e "  1. Configure OAuth: ${YELLOW}$BINARY_NAME config set --client-id <id> --client-secret <secret>${NC}"
    echo -e "  2. Create preset: ${YELLOW}$BINARY_NAME preset create <name> --refresh-token <token>${NC}"
    echo -e "  3. Explore GA4: ${YELLOW}$BINARY_NAME accounts list${NC}"
}

# Run main function
main "$@"