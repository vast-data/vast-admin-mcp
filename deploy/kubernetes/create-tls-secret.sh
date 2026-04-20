#!/bin/bash
# Create TLS Secret for VAST Admin MCP Server
#
# This script generates a self-signed TLS certificate and creates a Kubernetes
# TLS secret for the VAST Admin MCP deployment.
#
# Usage:
#   ./create-tls-secret.sh                    # Use defaults
#   ./create-tls-secret.sh -n my-namespace    # Custom namespace
#   ./create-tls-secret.sh -d 730             # Custom validity (2 years)
#   ./create-tls-secret.sh --help             # Show help
#
# Prerequisites:
#   - openssl
#   - kubectl configured with cluster access

set -e

# Default values
NAMESPACE="${NAMESPACE:-vast-admin-mcp}"
SECRET_NAME="${SECRET_NAME:-vast-admin-mcp-tls}"
COMMON_NAME="${COMMON_NAME:-vast-admin-mcp}"
DAYS="${DAYS:-365}"
KEY_SIZE="${KEY_SIZE:-2048}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Create a TLS secret for VAST Admin MCP Server.

Options:
    -n, --namespace NAME    Kubernetes namespace (default: vast-admin-mcp)
    -s, --secret NAME       Secret name (default: vast-admin-mcp-tls)
    -c, --cn NAME           Common Name for certificate (default: vast-admin-mcp)
    -d, --days DAYS         Certificate validity in days (default: 365)
    -k, --key-size SIZE     RSA key size in bits (default: 2048)
    --san NAMES             Additional Subject Alternative Names (comma-separated)
    -h, --help              Show this help message

Examples:
    $(basename "$0")
    $(basename "$0") -n production -d 730
    $(basename "$0") --san "mcp.example.com,10.0.0.1"

Environment Variables:
    NAMESPACE       Override default namespace
    SECRET_NAME     Override default secret name
    COMMON_NAME     Override default common name
    DAYS            Override default validity
    KEY_SIZE        Override default key size
EOF
    exit 0
}

# Parse arguments
EXTRA_SAN=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -s|--secret)
            SECRET_NAME="$2"
            shift 2
            ;;
        -c|--cn)
            COMMON_NAME="$2"
            shift 2
            ;;
        -d|--days)
            DAYS="$2"
            shift 2
            ;;
        -k|--key-size)
            KEY_SIZE="$2"
            shift 2
            ;;
        --san)
            EXTRA_SAN="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

# Check prerequisites
command -v openssl >/dev/null 2>&1 || { echo -e "${RED}Error: openssl is required but not installed.${NC}"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo -e "${RED}Error: kubectl is required but not installed.${NC}"; exit 1; }

# Create temporary directory
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo -e "${GREEN}Creating TLS certificate for VAST Admin MCP${NC}"
echo "================================================"
echo "Namespace:    $NAMESPACE"
echo "Secret Name:  $SECRET_NAME"
echo "Common Name:  $COMMON_NAME"
echo "Validity:     $DAYS days"
echo "Key Size:     $KEY_SIZE bits"
echo "================================================"

# Build SAN extension
SAN="DNS:${COMMON_NAME},DNS:localhost,IP:127.0.0.1"
if [[ -n "$EXTRA_SAN" ]]; then
    # Parse extra SANs (comma-separated)
    IFS=',' read -ra SANS <<< "$EXTRA_SAN"
    for san in "${SANS[@]}"; do
        san=$(echo "$san" | xargs)  # Trim whitespace
        if [[ "$san" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            SAN="${SAN},IP:${san}"
        else
            SAN="${SAN},DNS:${san}"
        fi
    done
fi

echo -e "${YELLOW}Subject Alternative Names: ${SAN}${NC}"

# Generate private key and certificate
echo -e "\n${GREEN}Generating certificate...${NC}"
openssl req -x509 -nodes -days "$DAYS" -newkey "rsa:$KEY_SIZE" \
    -keyout "$TMPDIR/tls.key" \
    -out "$TMPDIR/tls.crt" \
    -subj "/CN=$COMMON_NAME" \
    -addext "subjectAltName=$SAN" \
    2>/dev/null

# Verify the certificate
echo -e "${GREEN}Certificate details:${NC}"
openssl x509 -in "$TMPDIR/tls.crt" -noout -subject -dates -ext subjectAltName 2>/dev/null | head -10

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo -e "\n${YELLOW}Namespace '$NAMESPACE' does not exist. Creating...${NC}"
    kubectl create namespace "$NAMESPACE"
fi

# Create Kubernetes TLS secret
echo -e "\n${GREEN}Creating Kubernetes secret...${NC}"
kubectl create secret tls "$SECRET_NAME" \
    --cert="$TMPDIR/tls.crt" \
    --key="$TMPDIR/tls.key" \
    -n "$NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

echo -e "\n${GREEN}✅ TLS secret '${SECRET_NAME}' created successfully in namespace '${NAMESPACE}'${NC}"
echo -e "\nTo use this certificate, ensure your deployment mounts the secret:"
echo -e "  volumes:"
echo -e "  - name: tls-certs"
echo -e "    secret:"
echo -e "      secretName: ${SECRET_NAME}"
echo -e "\nTo view the secret:"
echo -e "  kubectl get secret ${SECRET_NAME} -n ${NAMESPACE}"
echo -e "\nTo renew the certificate, run this script again."
