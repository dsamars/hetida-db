// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! A real TLS handshake against a real server (§9).
//!
//! Everything else about TLS is testable without a certificate — that the
//! configuration refuses to guess, that half an identity is a mistake. This is
//! the part that is not: whether the PEM files an operator names actually
//! terminate a connection, and whether a client that does not speak TLS is kept
//! out of a listener that does.
//!
//! The certificate is generated here rather than checked in. A committed key is
//! a key, however clearly it is labelled a test one, and one with an expiry
//! turns a passing suite into a failing one on a date nobody chose.
//!
//! The whole chain is exercised, not just `tonic`: TOML text → `[server]`
//! section → [`TransportConfig`] → PEM read → `ServerTlsConfig` → handshake.
//! Starting from the file is the point, since that is where an operator's
//! mistake would be.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{FlightData, FlightDescriptor, HandshakeRequest, Ticket};
use futures::{TryStreamExt, stream};
use prost::Message;
use tokio::net::TcpListener;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Server};
use tonic::{Request, Status};

use hetida_db::archive::{Archive, ArchiveConfig};
use hetida_db::auth::{DEFAULT_TOKEN_TTL, TestTokens, TokenIssuer};
use hetida_db::clock::{Micros, TestClock};
use hetida_db::config::FileConfig;
use hetida_db::flight::{AUTHORIZATION, BearerInterceptor, HetidaFlightService};
use hetida_db::proto::Query;
use hetida_db::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, timestamp_type};
use hetida_db::serve::{AuthConfig, TransportConfig, tls_config};
use hetida_db::storage::InMemoryStorage;
use hetida_db::wal::WalConfig;

/// 2026-08-02T14:37:12Z.
const AUGUST: Micros = 1_785_681_432_000_000;
/// The name the certificate is issued for, and the one the client verifies.
const HOST: &str = "localhost";

/// A self-signed certificate and its key, as PEM, written into `directory`.
///
/// Returns the paths and the certificate itself, which the client needs as its
/// only trust root — the whole point being that it verifies rather than accepts
/// anything offered.
fn certificate(directory: &Path) -> (std::path::PathBuf, std::path::PathBuf, String) {
    let issued = rcgen::generate_simple_self_signed(vec![HOST.to_owned()])
        .expect("a self-signed certificate");
    let chain = issued.cert.pem();
    let key = issued.signing_key.serialize_pem();

    let chain_path = directory.join("chain.pem");
    let key_path = directory.join("key.pem");
    std::fs::write(&chain_path, &chain).expect("write chain");
    std::fs::write(&key_path, &key).expect("write key");

    (chain_path, key_path, chain)
}

/// A configuration naming that identity, read the way a start reads one.
fn configured(chain: &Path, key: &Path) -> FileConfig {
    FileConfig::parse(&format!(
        r#"
        [server]
        tls_certificate = "{}"
        tls_key = "{}"

        [[auth.users]]
        name = "ingest"
        password = "s3cret"
        read = ["readings"]
        write = ["readings"]
        "#,
        chain.display(),
        key.display(),
    ))
    .expect("configuration")
}

/// Serves an archive over TLS on an ephemeral port, returning the port.
async fn serve(file: &FileConfig) -> u16 {
    let storage = InMemoryStorage::new();
    let archive = Arc::new(
        Archive::open(
            Arc::new(storage.clone()),
            &ArchiveConfig {
                wal: WalConfig {
                    segment_bytes: 64 * 1024,
                    max_commit_delay: Duration::ZERO,
                },
                ..file.archive()
            },
            Arc::new(TestClock::new(AUGUST)),
        )
        .expect("archive"),
    );

    let AuthConfig::Users(credentials) = file.auth().expect("credentials") else {
        panic!("expected users");
    };
    let issuer = Arc::new(TokenIssuer::new(
        credentials,
        Arc::new(TestTokens::default()),
        Arc::new(TestClock::new(AUGUST)),
        DEFAULT_TOKEN_TTL,
    ));
    let service = HetidaFlightService::with_auth(archive, Arc::new(storage), Arc::clone(&issuer));

    // The chain under test: the file's `[server]` section becomes a transport
    // decision, which becomes the identity the listener presents.
    let transport = file.transport().expect("a TLS identity");
    assert!(
        matches!(transport, TransportConfig::Tls { .. }),
        "the file must have been read as TLS"
    );
    let tls = tls_config(&transport)
        .expect("the identity must load")
        .expect("TLS, not plaintext");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("address").port();
    tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .expect("the TLS identity was refused")
            .add_service(FlightServiceServer::with_interceptor(
                service,
                BearerInterceptor::new(issuer),
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .ok();
    });

    port
}

/// A client that trusts `chain` and nothing else.
async fn tls_client(port: u16, chain: &str) -> Result<FlightServiceClient<Channel>, String> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(chain))
        .domain_name(HOST);

    // `localhost` rather than the address, because that is the name in the
    // certificate — verifying it is the thing being tested.
    let channel = tonic::transport::Endpoint::from_shared(format!("https://{HOST}:{port}"))
        .map_err(|error| error.to_string())?
        .tls_config(tls)
        .map_err(|error| error.to_string())?
        .connect()
        .await
        .map_err(|error| error.to_string())?;
    Ok(FlightServiceClient::new(channel))
}

fn batch(rows: &[(u64, Micros, f64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

fn bearing<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    if let (Ok(key), Ok(value)) = (
        AUTHORIZATION.parse::<tonic::metadata::AsciiMetadataKey>(),
        format!("Bearer {token}").parse(),
    ) {
        request.metadata_mut().insert(key, value);
    }
    request
}

async fn handshake(client: &mut FlightServiceClient<Channel>) -> Result<String, Status> {
    let mut responses = client
        .handshake(stream::iter(vec![HandshakeRequest {
            protocol_version: 0,
            payload: b"ingest:s3cret".to_vec().into(),
        }]))
        .await?
        .into_inner();
    let first = responses
        .try_next()
        .await?
        .ok_or_else(|| Status::internal("no handshake response"))?;
    String::from_utf8(first.payload.to_vec()).map_err(|_| Status::internal("token is not UTF-8"))
}

/// §9: the whole point. A certificate an operator names in the configuration
/// terminates a real connection, and the archive works over it — handshake,
/// write, read.
#[tokio::test]
async fn a_configured_certificate_terminates_a_real_connection() {
    let directory = tempfile::tempdir().expect("temp dir");
    let (chain_path, key_path, chain) = certificate(directory.path());
    let file = configured(&chain_path, &key_path);

    let port = serve(&file).await;
    let mut client = tls_client(port, &chain).await.expect("a TLS connection");

    // The token is what TLS exists to protect, so obtaining one over TLS is the
    // first thing to confirm.
    let token = handshake(&mut client).await.expect("handshake over TLS");
    assert!(!token.is_empty());

    let encoded: Vec<FlightData> = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![
            "readings".to_owned(),
        ])))
        .build(stream::iter(vec![Ok(batch(&[(1, AUGUST, 1.5)]))]))
        .try_collect()
        .await
        .expect("encode");

    let mut acks = client
        .do_put(bearing(stream::iter(encoded), &token))
        .await
        .expect("do_put over TLS")
        .into_inner();
    let mut acknowledged = 0;
    while acks.try_next().await.expect("ack").is_some() {
        acknowledged += 1;
    }
    assert_eq!(acknowledged, 1);

    let ticket = Ticket {
        ticket: Query {
            sql: "SELECT * FROM readings".to_owned(),
            as_of_micros: None,
        }
        .encode_to_vec()
        .into(),
    };
    let incoming = client
        .do_get(bearing(ticket, &token))
        .await
        .expect("do_get over TLS")
        .into_inner();
    let batches: Vec<RecordBatch> =
        FlightRecordBatchStream::new_from_flight_data(incoming.map_err(FlightError::from))
            .try_collect()
            .await
            .expect("rows");

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

/// §9: a listener that terminates TLS must not also answer a client that does
/// not speak it. Otherwise the bearer token this protects would travel in the
/// clear for anyone who simply asked in plaintext.
#[tokio::test]
async fn a_plaintext_client_cannot_reach_a_tls_listener() {
    let directory = tempfile::tempdir().expect("temp dir");
    let (chain_path, key_path, _chain) = certificate(directory.path());
    let port = serve(&configured(&chain_path, &key_path)).await;

    let attempt = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await;

    // Either the connection is refused outright or the first call fails; both
    // are the listener declining to speak plaintext, and neither must succeed.
    match attempt {
        Err(_) => {}
        Ok(channel) => {
            let mut client = FlightServiceClient::new(channel);
            assert!(
                handshake(&mut client).await.is_err(),
                "a plaintext client completed a call against a TLS listener"
            );
        }
    }
}

/// §9: a client that does not trust the certificate must not connect either — a
/// server proving nothing is a server anyone can impersonate.
#[tokio::test]
async fn a_client_that_does_not_trust_the_certificate_is_refused() {
    let directory = tempfile::tempdir().expect("temp dir");
    let (chain_path, key_path, _chain) = certificate(directory.path());
    let port = serve(&configured(&chain_path, &key_path)).await;

    // A different certificate for the same name: correct shape, wrong issuer.
    let elsewhere = directory.path().join("other");
    std::fs::create_dir(&elsewhere).expect("temp dir");
    let other = certificate(&elsewhere);

    assert!(
        tls_client(port, &other.2).await.is_err(),
        "a certificate from another issuer must not be accepted"
    );
}

/// A named identity that is not there stops the start, rather than failing the
/// first connection that needs it.
#[test]
fn a_missing_certificate_is_reported_when_it_is_loaded() {
    let transport = TransportConfig::Tls {
        certificate: std::path::PathBuf::from("/nonexistent/chain.pem"),
        key: std::path::PathBuf::from("/nonexistent/key.pem"),
    };

    let error = tls_config(&transport).expect_err("must refuse");
    assert!(error.to_string().contains("chain.pem"), "{error}");
}

/// Plaintext loads no identity at all, which is what makes the two paths
/// distinguishable rather than one with an empty certificate.
#[test]
fn plaintext_loads_no_identity() {
    assert!(
        tls_config(&TransportConfig::Plaintext)
            .expect("no error")
            .is_none()
    );
}
