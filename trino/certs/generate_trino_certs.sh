#!/usr/bin/env bash
set -euo pipefail

# Generates:
# - Local CA certificate/key
# - Trino server certificate signed by that CA
# - PKCS#12 keystore used by Trino
#
# Usage:
#   ./trino/certs/generate_trino_certs.sh <keystore_password>
#
# Output files (in trino/certs):
# - ca.crt                 (import this into DBeaver trust store)
# - ca.key                 (keep private)
# - trino-server.crt
# - trino-server.key       (keep private)
# - trino_keystore.p12     (mounted into Trino)

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <keystore_password>" >&2
  exit 1
fi

KEYSTORE_PASSWORD="$1"
CERTS_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cat >"$TMP_DIR/san.ext" <<'EOF'
subjectAltName=DNS:trino,DNS:localhost,IP:127.0.0.1,IP:10.8.0.2
extendedKeyUsage=serverAuth
keyUsage=digitalSignature,keyEncipherment
basicConstraints=CA:FALSE
EOF

# CA
openssl genrsa -out "$CERTS_DIR/ca.key" 4096
openssl req -x509 -new -nodes \
  -key "$CERTS_DIR/ca.key" \
  -sha256 -days 3650 \
  -subj "/CN=wather-local-trino-ca" \
  -out "$CERTS_DIR/ca.crt"

# Server cert signed by CA
openssl genrsa -out "$CERTS_DIR/trino-server.key" 2048
openssl req -new \
  -key "$CERTS_DIR/trino-server.key" \
  -subj "/CN=trino" \
  -out "$TMP_DIR/trino-server.csr"

openssl x509 -req \
  -in "$TMP_DIR/trino-server.csr" \
  -CA "$CERTS_DIR/ca.crt" \
  -CAkey "$CERTS_DIR/ca.key" \
  -CAcreateserial \
  -out "$CERTS_DIR/trino-server.crt" \
  -days 825 -sha256 \
  -extfile "$TMP_DIR/san.ext"

# PKCS#12 keystore for Trino
openssl pkcs12 -export \
  -inkey "$CERTS_DIR/trino-server.key" \
  -in "$CERTS_DIR/trino-server.crt" \
  -certfile "$CERTS_DIR/ca.crt" \
  -name trino \
  -out "$CERTS_DIR/trino_keystore.p12" \
  -password "pass:${KEYSTORE_PASSWORD}"

echo "Generated certificates in: $CERTS_DIR"
echo "CA certificate for clients: $CERTS_DIR/ca.crt"
