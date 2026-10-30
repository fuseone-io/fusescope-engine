//! Handshakes against a TLS listener, with a plain rustls client so each
//! case controls exactly what the client presents and trusts.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines
)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fs_auth::{Reload, ServerTls, Tls, TlsError, TlsFiles, TlsLimits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
use tokio_stream::StreamExt;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn limits() -> TlsLimits {
    TlsLimits {
        reload_interval: Duration::from_secs(3600),
        handshake_timeout: Duration::from_secs(2),
        max_handshakes: 16,
    }
}

/// The three files copied under `dir`, so a test can rewrite them.
fn files_in(dir: &Path, cert: &str, key: &str, client_ca: &str) -> TlsFiles {
    let files = TlsFiles {
        cert: dir.join("tls.crt"),
        key: dir.join("tls.key"),
        client_ca: dir.join("ca.crt"),
    };
    install(&files, cert, key, client_ca);
    files
}

fn install(files: &TlsFiles, cert: &str, key: &str, client_ca: &str) {
    std::fs::copy(fixture(cert), &files.cert).unwrap();
    std::fs::copy(fixture(key), &files.key).unwrap();
    std::fs::copy(fixture(client_ca), &files.client_ca).unwrap();
}

/// Binds a listener whose accepted connections echo what they read.
async fn echo_server(tls: &ServerTls) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut incoming = tls.incoming(listener);
    tokio::spawn(async move {
        while let Some(stream) = incoming.next().await {
            let mut stream = stream.unwrap();
            tokio::spawn(async move {
                let mut buf = [0u8; 64];
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 || stream.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    addr
}

/// A client trusting `ca` for the server, presenting `identity` if any,
/// and expecting the server's certificate to name `host`.
async fn connect(
    addr: SocketAddr,
    host: &str,
    ca: &str,
    identity: Option<(&str, &str)>,
) -> std::io::Result<TlsStream<TcpStream>> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(fixture(ca)).unwrap() {
        roots.add(cert.unwrap()).unwrap();
    }
    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots);
    let config = match identity {
        Some((cert, key)) => builder
            .with_client_auth_cert(
                CertificateDer::pem_file_iter(fixture(cert))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect(),
                PrivateKeyDer::from_pem_file(fixture(key)).unwrap(),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    let tcp = TcpStream::connect(addr).await?;
    TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from(host.to_owned()).unwrap(), tcp)
        .await
}

/// One round trip on `stream`: the server answered, so it accepted us.
async fn round_trip(stream: &mut TlsStream<TcpStream>) -> std::io::Result<()> {
    stream.write_all(b"ping").await?;
    let mut buf = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
        .await
        .map_err(|_| std::io::Error::other("no answer"))??;
    assert_eq!(&buf, b"ping");
    Ok(())
}

/// Whether a client with `identity` gets an answer from the server.
async fn accepted(addr: SocketAddr, ca: &str, identity: Option<(&str, &str)>) -> bool {
    match connect(addr, "localhost", ca, identity).await {
        Ok(mut stream) => round_trip(&mut stream).await.is_ok(),
        Err(_) => false,
    }
}

fn leaf(stream: &TlsStream<TcpStream>) -> Vec<u8> {
    stream.get_ref().1.peer_certificates().unwrap()[0].to_vec()
}

fn fixture_leaf(name: &str) -> Vec<u8> {
    CertificateDer::pem_file_iter(fixture(name))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .to_vec()
}

#[test]
fn invalid_or_mismatched_files_refuse_the_load() {
    let good = || TlsFiles {
        cert: fixture("server-a.pem"),
        key: fixture("server-a.key"),
        client_ca: fixture("ca-a.pem"),
    };
    Tls::load(good(), limits()).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let garbage = dir.path().join("garbage.pem");
    std::fs::write(&garbage, b"not a pem file").unwrap();
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, b"").unwrap();
    let missing = dir.path().join("missing.pem");
    let cases = [
        (
            TlsFiles {
                cert: missing.clone(),
                ..good()
            },
            missing.clone(),
        ),
        (
            TlsFiles {
                cert: garbage.clone(),
                ..good()
            },
            garbage.clone(),
        ),
        (
            TlsFiles {
                key: garbage.clone(),
                ..good()
            },
            garbage.clone(),
        ),
        (
            TlsFiles {
                key: fixture("client-a.key"),
                ..good()
            },
            fixture("server-a.pem"),
        ),
        (
            TlsFiles {
                client_ca: empty.clone(),
                ..good()
            },
            empty.clone(),
        ),
        (
            TlsFiles {
                client_ca: fixture("server-a.key"),
                ..good()
            },
            fixture("server-a.key"),
        ),
    ];
    for (files, culprit) in cases {
        let err = Tls::load(files.clone(), limits()).unwrap_err();
        let path = match &err {
            TlsError::Read { path, .. } | TlsError::Invalid { path, .. } => path.clone(),
            TlsError::Endpoint { .. } => panic!("{err}"),
        };
        assert_eq!(path, culprit, "{files:?}: {err}");
    }
}

#[tokio::test]
async fn only_a_client_certified_by_the_client_ca_is_accepted() {
    let files = TlsFiles {
        cert: fixture("server-a.pem"),
        key: fixture("server-a.key"),
        client_ca: fixture("ca-a.pem"),
    };
    let tls = Tls::load(files, limits()).unwrap();
    let addr = echo_server(&tls.server()).await;

    assert!(accepted(addr, "ca-a.pem", Some(("client-a.pem", "client-a.key"))).await);
    assert!(
        !accepted(addr, "ca-a.pem", None).await,
        "no client certificate"
    );
    assert!(
        !accepted(addr, "ca-a.pem", Some(("client-b.pem", "client-b.key"))).await,
        "a client certificate from another CA"
    );
    let wrong_host = connect(
        addr,
        "other.test",
        "ca-a.pem",
        Some(("client-a.pem", "client-a.key")),
    )
    .await;
    assert!(
        wrong_host.is_err(),
        "the server's certificate names localhost"
    );
    assert!(
        connect(
            addr,
            "localhost",
            "ca-b.pem",
            Some(("client-a.pem", "client-a.key"))
        )
        .await
        .is_err(),
        "a server certificate the client does not trust"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(tls.stats().handshake_failures.load(Ordering::Relaxed) >= 4);
}

#[tokio::test]
async fn a_reload_serves_new_connections_and_leaves_open_ones() {
    let dir = tempfile::tempdir().unwrap();
    let files = files_in(dir.path(), "server-a.pem", "server-a.key", "ca-a.pem");
    let tls = Tls::load(files.clone(), limits()).unwrap();
    let addr = echo_server(&tls.server()).await;
    let mut open = connect(
        addr,
        "localhost",
        "ca-a.pem",
        Some(("client-a.pem", "client-a.key")),
    )
    .await
    .unwrap();
    round_trip(&mut open).await.unwrap();
    assert_eq!(leaf(&open), fixture_leaf("server-a.pem"));

    install(&files, "server-a2.pem", "server-a2.key", "ca-b.pem");
    assert_eq!(tls.reload().unwrap(), Reload::Reloaded);
    assert_eq!(tls.reload().unwrap(), Reload::Unchanged);
    assert_eq!(tls.stats().reloads.load(Ordering::Relaxed), 1);

    round_trip(&mut open)
        .await
        .expect("an open connection keeps the config it began with");
    assert!(
        !accepted(addr, "ca-a.pem", Some(("client-a.pem", "client-a.key"))).await,
        "the old client CA no longer admits new connections"
    );
    let mut rotated = connect(
        addr,
        "localhost",
        "ca-a.pem",
        Some(("client-b.pem", "client-b.key")),
    )
    .await
    .unwrap();
    round_trip(&mut rotated).await.unwrap();
    assert_eq!(
        leaf(&rotated),
        fixture_leaf("server-a2.pem"),
        "the same listener serves the new certificate"
    );
}

#[tokio::test]
async fn a_failed_reload_keeps_the_last_good_config_and_marks_it_stale() {
    let dir = tempfile::tempdir().unwrap();
    let files = files_in(dir.path(), "server-a.pem", "server-a.key", "ca-a.pem");
    let tls = Tls::load(files.clone(), limits()).unwrap();
    let addr = echo_server(&tls.server()).await;
    let stats = tls.stats();

    std::fs::write(&files.client_ca, b"broken").unwrap();
    assert!(tls.reload().is_err());
    assert!(stats.stale.load(Ordering::Relaxed));
    assert_eq!(stats.reload_failures.load(Ordering::Relaxed), 1);
    assert!(
        accepted(addr, "ca-a.pem", Some(("client-a.pem", "client-a.key"))).await,
        "the last good config still serves"
    );
    assert_eq!(
        tls.reload().unwrap(),
        Reload::Unchanged,
        "contents that failed are not tried again"
    );
    assert!(stats.stale.load(Ordering::Relaxed));

    std::fs::copy(fixture("client-a.key"), &files.key).unwrap();
    std::fs::copy(fixture("ca-a.pem"), &files.client_ca).unwrap();
    assert!(
        tls.reload().is_err(),
        "a key that does not match the certificate"
    );
    std::fs::remove_file(&files.key).unwrap();
    assert!(tls.reload().is_err(), "a key file that is gone");
    assert_eq!(stats.reload_failures.load(Ordering::Relaxed), 3);
    assert!(accepted(addr, "ca-a.pem", Some(("client-a.pem", "client-a.key"))).await);

    install(&files, "server-a.pem", "server-a.key", "ca-a.pem");
    assert_eq!(tls.reload().unwrap(), Reload::Reloaded);
    assert!(!stats.stale.load(Ordering::Relaxed));
}

#[tokio::test]
async fn the_reload_task_follows_the_files_until_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let files = files_in(dir.path(), "server-a.pem", "server-a.key", "ca-a.pem");
    let tls = Tls::load(
        files.clone(),
        TlsLimits {
            reload_interval: Duration::from_millis(20),
            ..limits()
        },
    )
    .unwrap();
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(Arc::clone(&tls).run(shutdown));
    install(&files, "server-a2.pem", "server-a2.key", "ca-a.pem");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tls.stats().reloads.load(Ordering::Relaxed) == 0 {
        assert!(tokio::time::Instant::now() < deadline, "never reloaded");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the task stops with the shutdown")
        .unwrap();
}

#[tokio::test]
async fn silent_and_excess_connections_are_dropped_within_the_limits() {
    let files = TlsFiles {
        cert: fixture("server-a.pem"),
        key: fixture("server-a.key"),
        client_ca: fixture("ca-a.pem"),
    };
    let tls = Tls::load(
        files,
        TlsLimits {
            handshake_timeout: Duration::from_millis(300),
            max_handshakes: 1,
            ..limits()
        },
    )
    .unwrap();
    let addr = echo_server(&tls.server()).await;
    let stats = tls.stats();
    let mut silent = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut excess = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 1];
    let closed = tokio::time::timeout(Duration::from_secs(2), excess.read(&mut buf))
        .await
        .unwrap();
    assert!(matches!(closed, Ok(0) | Err(_)), "closed at once");
    assert_eq!(stats.handshakes_refused.load(Ordering::Relaxed), 1);
    let closed = tokio::time::timeout(Duration::from_secs(2), silent.read(&mut buf))
        .await
        .unwrap();
    assert!(matches!(closed, Ok(0) | Err(_)), "closed at the timeout");
    assert_eq!(stats.handshake_failures.load(Ordering::Relaxed), 1);
    assert!(
        accepted(addr, "ca-a.pem", Some(("client-a.pem", "client-a.key"))).await,
        "the permit came back"
    );
}
