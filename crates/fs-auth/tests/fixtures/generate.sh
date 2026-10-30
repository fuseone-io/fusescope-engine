#!/bin/sh
# Regenerates the TLS test fixtures: two CAs, two server identities for
# localhost signed by CA a, and one client identity per CA. Valid for a
# century so the tests never age; never used outside tests.
set -eu
cd "$(dirname "$0")"
days=36500

ca() {
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout "$1.key" -out "$1.pem" -days "$days" -subj "/CN=fusescope test $1" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign"
}

leaf() {
    name=$1 issuer=$2 san=$3 usage=$4
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout "$name.key" -out "$name.csr" -subj "/CN=$name"
    printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=%s\nsubjectAltName=%s\n' \
        "$usage" "$san" > "$name.ext"
    openssl x509 -req -in "$name.csr" -CA "$issuer.pem" -CAkey "$issuer.key" \
        -CAcreateserial -out "$name.pem" -days "$days" -extfile "$name.ext"
    rm "$name.csr" "$name.ext"
}

ca ca-a
ca ca-b
leaf server-a ca-a DNS:localhost serverAuth,clientAuth
leaf server-a2 ca-a DNS:localhost serverAuth,clientAuth
leaf client-a ca-a DNS:client.test clientAuth
leaf client-b ca-b DNS:client.test clientAuth
rm -f ./*.srl ca-a.key ca-b.key
