#!/bin/sh
# Regenerates the TLS test fixtures. Valid for a century so the tests
# never age; never used outside tests.
#
# ca-a, ca-a2, ca-b, ca-b2   independent self-signed roots
# ca-a-reissued              ca-a's subject and key, another serial
# ca-a-renamed               ca-a's key under another subject
# ca-a-intermediate          a CA signed by ca-a, not self-signed
# ca-a-lookalike             ca-a's subject on another key, signed by ca-a:
#                            self-issued, not self-signed
# server-a, server-a2        identities for localhost, issued by ca-a
# client-X                   a client identity issued by ca-X
set -eu
cd "$(dirname "$0")"
days=36500

root() {
    name=$1 key=${2:-$1} subject=${3:-$1}
    [ -f "$key.key" ] || openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$key.key"
    openssl req -x509 -new -key "$key.key" -out "$name.pem" -days "$days" \
        -subj "/CN=fusescope test $subject" -set_serial "0x$(openssl rand -hex 8)" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign"
}

issue() {
    name=$1 issuer=$2 ext=$3
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout "$name.key" -out "$name.csr" -subj "/CN=$name"
    printf '%b' "$ext" > "$name.ext"
    openssl x509 -req -in "$name.csr" -CA "$issuer.pem" -CAkey "$issuer.key" \
        -set_serial "0x$(openssl rand -hex 8)" -out "$name.pem" -days "$days" -extfile "$name.ext"
    rm "$name.csr" "$name.ext"
}

lookalike() {
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout "$1.key" -out "$1.csr" -subj "/CN=fusescope test $2"
    printf 'basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n' > "$1.ext"
    openssl x509 -req -in "$1.csr" -CA "$2.pem" -CAkey "$2.key" \
        -set_serial "0x$(openssl rand -hex 8)" -out "$1.pem" -days "$days" -extfile "$1.ext"
    rm "$1.csr" "$1.ext" "$1.key"
}

leaf() {
    issue "$1" "$2" "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=$4\nsubjectAltName=$3\n"
}

rm -f ./*.pem ./*.key
root ca-a
root ca-a-reissued ca-a ca-a
root ca-a-renamed ca-a ca-a-renamed
root ca-a2
root ca-b
root ca-b2
issue ca-a-intermediate ca-a "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n"
lookalike ca-a-lookalike ca-a
leaf server-a ca-a DNS:localhost serverAuth,clientAuth
leaf server-a2 ca-a DNS:localhost serverAuth,clientAuth
leaf client-a ca-a DNS:client.test clientAuth
leaf client-a2 ca-a2 DNS:client.test clientAuth
leaf client-b ca-b DNS:client.test clientAuth
leaf client-b2 ca-b2 DNS:client.test clientAuth
rm -f ca-a.key ca-a2.key ca-b.key ca-b2.key ca-a-intermediate.key
