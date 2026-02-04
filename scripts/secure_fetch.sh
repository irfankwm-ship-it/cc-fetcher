#!/bin/bash
# Secure fetch pipeline
#
# This script:
# 1. Runs the fetcher in an isolated Docker container
# 2. Validates the output for malicious content
# 3. Only moves validated data to the main data store
#
# Usage:
#   ./scripts/secure_fetch.sh [date]
#   ./scripts/secure_fetch.sh 2026-02-03

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATE="${1:-$(date -u +%Y-%m-%d)}"
STAGING_DIR="/tmp/cc-fetcher-staging/${DATE}"
FINAL_DIR="${CC_DATA_DIR:-../cc-data}/raw/${DATE}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Cleanup on exit
cleanup() {
    if [[ -d "$STAGING_DIR" ]]; then
        log_info "Cleaning up staging directory..."
        rm -rf "$STAGING_DIR"
    fi
}
trap cleanup EXIT

# Create isolated staging directory
log_info "Creating staging directory: $STAGING_DIR"
mkdir -p "$STAGING_DIR"
chmod 700 "$STAGING_DIR"

# Build the Docker image if needed
log_info "Building Docker image..."
cd "$PROJECT_DIR"
docker compose build --quiet fetcher

# Run fetcher in isolated container
log_info "Running fetcher for date: $DATE"
docker compose run --rm \
    -v "$STAGING_DIR:/data/raw:rw" \
    -e CC_ENV="${CC_ENV:-prod}" \
    fetcher run \
    --date "$DATE" \
    --output-dir /data/raw

# Check if any data was fetched
if [[ ! -d "$STAGING_DIR/$DATE" ]] || [[ -z "$(ls -A "$STAGING_DIR/$DATE" 2>/dev/null)" ]]; then
    log_warn "No data fetched for $DATE"
    exit 0
fi

# Validate the fetched data
log_info "Validating fetched data..."
if ! python3 "$SCRIPT_DIR/validate_output.py" "$STAGING_DIR/$DATE"; then
    log_error "Validation FAILED - data will NOT be moved to main storage"
    log_error "Suspicious content detected in fetched data"

    # Optionally save failed data for review
    QUARANTINE_DIR="${QUARANTINE_DIR:-/tmp/cc-fetcher-quarantine}"
    mkdir -p "$QUARANTINE_DIR"
    mv "$STAGING_DIR/$DATE" "$QUARANTINE_DIR/${DATE}-$(date +%s)"
    log_warn "Failed data quarantined to: $QUARANTINE_DIR"

    exit 1
fi

# Move validated data to final location
log_info "Validation PASSED - moving data to: $FINAL_DIR"
mkdir -p "$(dirname "$FINAL_DIR")"

# Use rsync for atomic-ish transfer with checksums
if command -v rsync &> /dev/null; then
    rsync -a --checksum "$STAGING_DIR/$DATE/" "$FINAL_DIR/"
else
    cp -r "$STAGING_DIR/$DATE/." "$FINAL_DIR/"
fi

# Set restrictive permissions on final data
chmod 644 "$FINAL_DIR"/*.json 2>/dev/null || true
chmod 755 "$FINAL_DIR"

log_info "Fetch complete! Data available at: $FINAL_DIR"

# List fetched files
echo ""
log_info "Fetched files:"
ls -la "$FINAL_DIR"
