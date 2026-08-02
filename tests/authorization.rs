// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! System-level verification of authentication and authorization (§9).
//!
//! A real `tonic` server on an ephemeral port, the real interceptor, real
//! credentials — and, unlike the unit tests beside `flight.rs`, data that has
//! already reached Parquet. A grant has to hold over rows the query engine
//! reads off disk exactly as it does over rows still in a memtable, and the two
//! reach the scan by different paths.
//!
//! Deterministic throughout: port 0 so a busy machine cannot make this flaky,
//! and an injected clock so a token can be expired without waiting for one.

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
use tonic::transport::{Channel, Server};
use tonic::{Request, Status};

use hetida_db::archive::{Archive, ArchiveConfig};
use hetida_db::auth::{
    DEFAULT_TOKEN_TTL, Grants, Principal, StaticCredentials, TableSet, TestTokens, TokenIssuer,
};
use hetida_db::clock::{Micros, TestClock};
use hetida_db::flight::{AUTHORIZATION, BearerInterceptor, HetidaFlightService};
use hetida_db::flush::Flusher;
use hetida_db::parquet::ParquetConfig;
use hetida_db::proto::Query;
use hetida_db::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, timestamp_type};
use hetida_db::storage::InMemoryStorage;
use hetida_db::wal::WalConfig;

/// 2026-08-02T14:37:12Z.
const AUGUST: Micros = 1_785_681_432_000_000;

/// A served archive, with the handles a test needs to drive it.
struct Served {
    client: FlightServiceClient<Channel>,
    archive: Arc<Archive>,
    storage: InMemoryStorage,
    clock: Arc<TestClock>,
}

/// Serves an archive behind the real interceptor, on an ephemeral port.
async fn serve() -> Served {
    let storage = InMemoryStorage::new();
    let clock = Arc::new(TestClock::new(AUGUST));
    let archive = Arc::new(
        Archive::open(
            Arc::new(storage.clone()),
            &ArchiveConfig {
                wal: WalConfig {
                    segment_bytes: 64 * 1024,
                    max_commit_delay: Duration::ZERO,
                },
                ..ArchiveConfig::default()
            },
            Arc::clone(&clock) as Arc<_>,
        )
        .expect("archive"),
    );

    let credentials = Arc::new(
        StaticCredentials::new()
            .with_user("admin", "s3cret", Grants::all())
            .with_user("reader", "hunter2", Grants::read_only(["readings"]))
            .with_user(
                "events-only",
                "pw",
                Grants {
                    read: TableSet::named(["events"]),
                    write: TableSet::named(["events"]),
                },
            ),
    );
    let issuer = Arc::new(TokenIssuer::new(
        credentials,
        Arc::new(TestTokens::default()),
        Arc::clone(&clock) as Arc<_>,
        DEFAULT_TOKEN_TTL,
    ));

    let service = HetidaFlightService::with_auth(
        Arc::clone(&archive),
        Arc::new(storage.clone()),
        Arc::clone(&issuer),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::with_interceptor(
                service,
                BearerInterceptor::new(issuer),
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .ok();
    });

    let endpoint = format!("http://{address}");
    let client = loop {
        match tonic::transport::Endpoint::from_shared(endpoint.clone())
            .expect("endpoint")
            .connect()
            .await
        {
            Ok(channel) => break FlightServiceClient::new(channel),
            // Yielding rather than sleeping keeps this deterministic.
            Err(_) => tokio::task::yield_now().await,
        }
    };

    Served {
        client,
        archive,
        storage,
        clock,
    }
}

impl Served {
    async fn handshake(&mut self, user: &str, password: &str) -> Result<String, Status> {
        let mut responses = self
            .client
            .handshake(stream::iter(vec![HandshakeRequest {
                protocol_version: 0,
                payload: format!("{user}:{password}").into_bytes().into(),
            }]))
            .await?
            .into_inner();
        let first = responses
            .try_next()
            .await?
            .ok_or_else(|| Status::internal("no handshake response"))?;
        String::from_utf8(first.payload.to_vec())
            .map_err(|_| Status::internal("token is not UTF-8"))
    }

    async fn write(
        &mut self,
        table: &str,
        rows: &[(u64, Micros, f64)],
        token: Option<&str>,
    ) -> Result<usize, Status> {
        let encoded: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![table.to_owned()])))
            .build(stream::iter(vec![Ok(batch(rows))]))
            .try_collect()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        let mut acks = self
            .client
            .do_put(bearing(stream::iter(encoded), token))
            .await?
            .into_inner();
        let mut count = 0;
        while acks.try_next().await?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    async fn read(&mut self, sql: &str, token: Option<&str>) -> Result<usize, Status> {
        let ticket = Ticket {
            ticket: Query {
                sql: sql.to_owned(),
                as_of_micros: None,
            }
            .encode_to_vec()
            .into(),
        };
        let incoming = self
            .client
            .do_get(bearing(ticket, token))
            .await?
            .into_inner();
        let batches: Vec<RecordBatch> =
            FlightRecordBatchStream::new_from_flight_data(incoming.map_err(FlightError::from))
                .try_collect()
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
        Ok(batches.iter().map(RecordBatch::num_rows).sum())
    }

    /// Moves every partition of `table` into Parquet.
    fn flush(&self, table: &str) {
        let state = self.archive.table(table).expect("table");
        let flusher = Flusher::new(
            Arc::new(self.storage.clone()),
            table,
            self.archive.interval(),
            ParquetConfig::default(),
        );
        for start in state.partition_starts() {
            flusher.flush(&state, start).expect("flush");
        }
        assert_eq!(state.num_rows(), 0, "the rows must now be on disk only");
    }
}

/// Attaches a bearer token to a request, as a client does on every call.
fn bearing<T>(message: T, token: Option<&str>) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(token) = token
        && let (Ok(key), Ok(value)) = (
            AUTHORIZATION.parse::<tonic::metadata::AsciiMetadataKey>(),
            format!("Bearer {token}").parse(),
        )
    {
        request.metadata_mut().insert(key, value);
    }
    request
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

/// §9: nothing is served to a caller who has not authenticated — in either
/// direction, and whatever the archive happens to hold.
#[tokio::test]
async fn an_unauthenticated_caller_is_refused_on_both_directions() {
    let mut served = serve().await;

    assert_eq!(
        served
            .write("readings", &[(1, AUGUST, 1.5)], None)
            .await
            .expect_err("must be refused")
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        served
            .read("SELECT * FROM readings", None)
            .await
            .expect_err("must be refused")
            .code(),
        tonic::Code::Unauthenticated
    );
    assert!(
        served.archive.table("readings").is_none(),
        "a refused write must not have created the table"
    );
}

/// §9: the grant holds over rows that have already reached Parquet. Memory and
/// disk reach the scan by different paths, and only one of them is exercised by
/// a test that never flushes.
#[tokio::test]
async fn grants_hold_over_rows_read_back_from_parquet() {
    let mut served = serve().await;
    let admin = served.handshake("admin", "s3cret").await.expect("admin");

    served
        .write("readings", &[(1, AUGUST, 1.5)], Some(&admin))
        .await
        .expect("write readings");
    served
        .write("events", &[(2, AUGUST, 2.5)], Some(&admin))
        .await
        .expect("write events");
    served.flush("readings");
    served.flush("events");

    let reader = served.handshake("reader", "hunter2").await.expect("reader");

    assert_eq!(
        served
            .read("SELECT * FROM readings", Some(&reader))
            .await
            .expect("granted"),
        1,
        "the granted table must still answer from disk"
    );
    assert_eq!(
        served
            .read("SELECT * FROM events", Some(&reader))
            .await
            .expect_err("must be refused")
            .code(),
        tonic::Code::PermissionDenied,
        "and the ungranted one must not, however it is stored"
    );
}

/// §9: per-table grants on both directions, checked against the table a
/// descriptor names and the tables a query reads.
#[tokio::test]
async fn grants_are_enforced_per_table_on_do_put_and_do_get() {
    let mut served = serve().await;
    let admin = served.handshake("admin", "s3cret").await.expect("admin");
    served
        .write("readings", &[(1, AUGUST, 1.5)], Some(&admin))
        .await
        .expect("seed");
    served
        .write("events", &[(2, AUGUST, 2.5)], Some(&admin))
        .await
        .expect("seed");

    let scoped = served
        .handshake("events-only", "pw")
        .await
        .expect("events-only");

    assert!(
        served
            .write("events", &[(3, AUGUST, 3.5)], Some(&scoped))
            .await
            .is_ok(),
        "granted write"
    );
    assert_eq!(
        served
            .write("readings", &[(3, AUGUST, 3.5)], Some(&scoped))
            .await
            .expect_err("must be refused")
            .code(),
        tonic::Code::PermissionDenied
    );
    assert!(
        served
            .read("SELECT * FROM events", Some(&scoped))
            .await
            .is_ok(),
        "granted read"
    );
    assert_eq!(
        served
            .read("SELECT * FROM readings", Some(&scoped))
            .await
            .expect_err("must be refused")
            .code(),
        tonic::Code::PermissionDenied
    );
}

/// §9: a read grant is not a write grant. The commonest misconfiguration there
/// is, and the one that would be least visible if it were wrong.
#[tokio::test]
async fn a_read_grant_does_not_admit_a_write() {
    let mut served = serve().await;
    let reader = served.handshake("reader", "hunter2").await.expect("reader");

    assert_eq!(
        served
            .write("readings", &[(1, AUGUST, 1.5)], Some(&reader))
            .await
            .expect_err("must be refused")
            .code(),
        tonic::Code::PermissionDenied
    );
}

/// §9: a token expires. Driven by the injected clock, so this is a test rather
/// than a twelve-hour wait — and the session must be refused, not merely
/// reported as old.
#[tokio::test]
async fn a_token_stops_working_once_it_expires() {
    let mut served = serve().await;
    let admin = served.handshake("admin", "s3cret").await.expect("admin");

    assert!(
        served
            .write("readings", &[(1, AUGUST, 1.5)], Some(&admin))
            .await
            .is_ok(),
        "valid while fresh"
    );

    served.clock.advance(DEFAULT_TOKEN_TTL);

    assert_eq!(
        served
            .write("readings", &[(2, AUGUST, 2.5)], Some(&admin))
            .await
            .expect_err("must be refused")
            .code(),
        tonic::Code::Unauthenticated,
        "an expired token is not a token"
    );
}

/// §9: the credential store is a seam, reachable and implementable from outside
/// the crate — which is the whole claim behind "OIDC and mTLS are later
/// backends".
#[test]
fn the_credential_store_can_be_implemented_from_outside() {
    use hetida_db::auth::CredentialStore;

    #[derive(Debug)]
    struct FromElsewhere;

    impl CredentialStore for FromElsewhere {
        fn authenticate(&self, user: &str, _password: &str) -> Option<Principal> {
            Some(Principal::new(user, Grants::read_only(["readings"])))
        }
    }

    let store: Arc<dyn CredentialStore> = Arc::new(FromElsewhere);
    let principal = store.authenticate("anyone", "anything").expect("accepted");

    assert!(principal.may_read("readings").is_ok());
    assert!(principal.may_write("readings").is_err());
}
