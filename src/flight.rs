// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The Arrow Flight surface (§5).
//!
//! One [`FlightService`] over an [`Archive`]. Every method the archive does not
//! serve yet answers `UNIMPLEMENTED` rather than being absent, which is what a
//! generic Flight client expects to be told.
//!
//! # Registration
//!
//! [`REGISTER_ACTION`] resolves series identities to `u64` ids. It is a
//! `do_action` rather than part of `do_put` because §10 keeps name resolution off
//! the ingest path entirely: clients resolve once, cache, and send ids. The call
//! is batched, so a client resolves everything it will write in one round trip.
//!
//! Registration is durable before it answers — the id is in the catalogue log
//! (§4) by the time the client sees it, so a client that caches an id can rely on
//! the server still knowing it after a crash.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::archive::{Archive, IngestError};
use crate::proto::{RegisterRequest, RegisterResponse};
use crate::registry::SeriesKey;

/// Action type resolving series identities to ids (§10).
pub const REGISTER_ACTION: &str = "register";

/// Arrow Flight service over one archive.
#[derive(Debug)]
pub struct HetidaFlightService {
    archive: Arc<Archive>,
}

impl HetidaFlightService {
    /// Serves `archive` over Flight.
    pub fn new(archive: Arc<Archive>) -> Self {
        Self { archive }
    }

    /// The archive this service serves.
    pub fn archive(&self) -> &Arc<Archive> {
        &self.archive
    }

    /// Resolves every series in the request, in request order (§10).
    ///
    /// A single failure fails the whole call: a client cannot use a partial
    /// answer, because it maps ids back to its own request by position.
    fn register(&self, body: &[u8]) -> Result<RegisterResponse, Status> {
        let request = RegisterRequest::decode(body).map_err(|error| {
            Status::invalid_argument(format!("malformed register request: {error}"))
        })?;

        if request.series.is_empty() {
            return Err(Status::invalid_argument(
                "register requires at least one series",
            ));
        }

        let mut metric_ids = Vec::with_capacity(request.series.len());
        for series in &request.series {
            if series.name.is_empty() {
                return Err(Status::invalid_argument("a series name may not be empty"));
            }
            // The wire form carries labels as a map, so the order they arrived in
            // is not trusted: `SeriesKey` canonicalises them (§10).
            let key = SeriesKey::new(
                series.name.clone(),
                series
                    .labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone())),
            );
            metric_ids.push(
                self.archive
                    .registry()
                    .register(key)
                    // The registry only fails when the id could not be made
                    // durable, which is the server's problem, not the client's.
                    .map_err(|error| Status::internal(format!("registration failed: {error}")))?,
            );
        }

        Ok(RegisterResponse { metric_ids })
    }
}

/// The table a descriptor path names (§5).
///
/// Exactly one path element: a table name, not a hierarchy. Accepting a longer
/// path would leave it ambiguous which element the table is.
fn table_from(path: &[String]) -> Result<&str, Status> {
    match path {
        [table] if !table.is_empty() => Ok(table),
        [] => Err(Status::invalid_argument(
            "do_put needs a table in FlightDescriptor::path (§5)",
        )),
        _ => Err(Status::invalid_argument(format!(
            "expected one path element naming a table, got {}",
            path.len()
        ))),
    }
}

/// Maps an ingest failure onto the status a client should see.
///
/// The distinction matters: `RESOURCE_EXHAUSTED` tells a client to back off and
/// retry (§7), where `INVALID_ARGUMENT` tells it the batch will never be
/// accepted and retrying is pointless.
fn ingest_status(error: IngestError) -> Status {
    match error {
        IngestError::Exhausted { .. } => Status::resource_exhausted(error.to_string()),
        IngestError::Catalogue(_) => Status::invalid_argument(error.to_string()),
        IngestError::Stamp(_) | IngestError::Wal(_) | IngestError::Table(_) => {
            Status::internal(error.to_string())
        }
    }
}

/// A stream of one item, which is what an action result is here.
fn one<T: Send + 'static>(item: T) -> BoxStream<'static, Result<T, Status>> {
    Box::pin(stream::once(async move { Ok(item) }))
}

#[tonic::async_trait]
impl FlightService for HetidaFlightService {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send>>;

    /// Resolves ids for a batch of series identities (§10).
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        if action.r#type != REGISTER_ACTION {
            return Err(Status::unimplemented(format!(
                "unknown action `{}`; this server serves `{REGISTER_ACTION}`",
                action.r#type
            )));
        }

        let response = self.register(&action.body)?;
        Ok(Response::new(one(arrow_flight::Result {
            body: response.encode_to_vec().into(),
        })))
    }

    /// Describes the actions this server serves, so a client can discover them.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(one(ActionType {
            r#type: REGISTER_ACTION.to_owned(),
            description: "Resolve metric names and label sets to u64 metric ids. \
                          Body is a hetida.v1.RegisterRequest, result a \
                          hetida.v1.RegisterResponse."
                .to_owned(),
        })))
    }

    // ---- not yet served (§5) ----

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "handshake arrives with authentication (§9)",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "list_flights arrives with the query engine (§5)",
        ))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info arrives with the query engine (§5)",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "long-running queries are not part of v1 (§5)",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "get_schema arrives with the query engine (§5)",
        ))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get arrives with the query engine (§5)",
        ))
    }

    /// Ingests a stream of `RecordBatch` into the table the descriptor names
    /// (§4, §5).
    ///
    /// One acknowledgement per batch, sent only after the fsync its record was
    /// part of returned — *acked* means *durable* (§4). The stream carries many
    /// batches behind a single call, which is why authorization belongs on the
    /// call and not on the message (§9).
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let archive = Arc::clone(&self.archive);
        let mut incoming = request.into_inner().peekable();

        // The descriptor rides on the first message, so it is read before the
        // stream is handed to the decoder (§5).
        let path = match Pin::new(&mut incoming).peek().await {
            Some(Ok(first)) => first
                .flight_descriptor
                .as_ref()
                .map(|descriptor| descriptor.path.clone())
                .unwrap_or_default(),
            Some(Err(status)) => return Err(status.clone()),
            None => return Err(Status::invalid_argument("do_put received no data")),
        };
        let table = table_from(&path)?.to_owned();

        let mut batches =
            FlightRecordBatchStream::new_from_flight_data(incoming.map_err(FlightError::from));

        // Acknowledgements are produced lazily, one per batch, as each is made
        // durable. Collecting them first would read the whole stream into memory
        // before answering anything — which is precisely the unbounded buffering
        // §7 forbids. Because the stream is only polled as the client reads
        // acknowledgements, gRPC flow control carries the pressure back to it
        // without a protocol extension.
        Ok(Response::new(Box::pin(stream::poll_fn(move |context| {
            batches.poll_next_unpin(context).map(|batch| {
                batch.map(|batch| {
                    let batch = batch.map_err(|error| {
                        Status::invalid_argument(format!("malformed flight data: {error}"))
                    })?;
                    let outcome = archive.ingest(&table, &batch).map_err(ingest_status)?;
                    Ok(PutResult {
                        app_metadata: format!(
                            "{{\"inserted\":{},\"suppressed\":{}}}",
                            outcome.inserted, outcome.suppressed
                        )
                        .into_bytes()
                        .into(),
                    })
                })
            })
        }))))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "do_exchange is not part of the surface (§5)",
        ))
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable
)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::flight_service_server::FlightServiceServer;
    use tokio::net::TcpListener;
    use tonic::transport::Server;

    use crate::archive::{Archive, ArchiveConfig};
    use crate::clock::TestClock;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::proto::SeriesKey as ProtoKey;
    use crate::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, timestamp_type};
    use crate::storage::InMemoryStorage;
    use crate::wal::WalConfig;

    /// Starts a server on an ephemeral port and returns a client for it.
    ///
    /// Port 0 rather than a fixed one: two tests running in parallel must not
    /// collide, and a hardcoded port makes a suite flaky on a busy machine.
    async fn serve() -> (FlightServiceClient<tonic::transport::Channel>, Arc<Archive>) {
        let storage = Arc::new(InMemoryStorage::new());
        let archive = Arc::new(
            Archive::open(
                storage,
                &ArchiveConfig {
                    wal: WalConfig {
                        segment_bytes: 64 * 1024,
                        max_commit_delay: Duration::ZERO,
                    },
                    ..ArchiveConfig::default()
                },
                Arc::new(TestClock::new(1_000)),
            )
            .expect("archive"),
        );
        serve_archive(archive).await
    }

    /// Serves an archive the caller built, for tests that need a specific config.
    async fn serve_archive(
        archive: Arc<Archive>,
    ) -> (FlightServiceClient<tonic::transport::Channel>, Arc<Archive>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let service = HetidaFlightService::new(Arc::clone(&archive));

        tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(service))
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
                // The spawned server may not be listening yet. Yielding rather
                // than sleeping keeps the test deterministic.
                Err(_) => tokio::task::yield_now().await,
            }
        };

        (client, archive)
    }

    /// A client batch: identity columns plus a value, no system columns.
    fn client_batch(rows: &[(u64, i64, f64)]) -> arrow::array::RecordBatch {
        use arrow::array::{Float64Array, TimestampMicrosecondArray, UInt64Array};
        arrow::array::RecordBatch::try_new(
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
        .expect("client batch")
    }

    /// Encodes batches as Flight data without a descriptor.
    fn encode_without_descriptor(batch: &arrow::array::RecordBatch) -> (Vec<FlightData>, ()) {
        let encoded: Vec<FlightData> = futures::executor::block_on(async {
            arrow_flight::encode::FlightDataEncoderBuilder::new()
                .build(stream::iter(vec![Ok(batch.clone())]))
                .try_collect()
                .await
                .expect("encode")
        });
        (encoded, ())
    }

    /// Writes `batches` into `table` and returns how many acknowledgements came
    /// back.
    async fn write(
        client: &mut FlightServiceClient<tonic::transport::Channel>,
        table: &str,
        batches: Vec<arrow::array::RecordBatch>,
    ) -> Result<usize, Status> {
        let encoded: Vec<FlightData> = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![table.to_owned()])))
            .build(stream::iter(batches.into_iter().map(Ok)))
            .try_collect()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        let mut acks = client.do_put(stream::iter(encoded)).await?.into_inner();
        let mut count = 0;
        while futures::TryStreamExt::try_next(&mut acks).await?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    fn key(name: &str, labels: &[(&str, &str)]) -> ProtoKey {
        ProtoKey {
            name: name.to_owned(),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect::<HashMap<_, _>>(),
        }
    }

    async fn register(
        client: &mut FlightServiceClient<tonic::transport::Channel>,
        series: Vec<ProtoKey>,
    ) -> Result<Vec<u64>, Status> {
        let response = client
            .do_action(Action {
                r#type: REGISTER_ACTION.to_owned(),
                body: RegisterRequest { series }.encode_to_vec().into(),
            })
            .await?;

        let mut results = response.into_inner();
        let first = futures::TryStreamExt::try_next(&mut results)
            .await?
            .ok_or_else(|| Status::internal("no result"))?;

        Ok(RegisterResponse::decode(first.body.as_ref())
            .map_err(|error| Status::internal(error.to_string()))?
            .metric_ids)
    }

    /// §10: clients resolve names to ids once, over Flight, and cache them.
    #[tokio::test]
    async fn registering_over_flight_returns_ids() {
        let (mut client, _archive) = serve().await;

        let ids = register(&mut client, vec![key("temp", &[("plant", "a")])])
            .await
            .expect("register");

        assert_eq!(ids.len(), 1);
    }

    /// §10: the call is batched, and ids come back in request order — a client
    /// maps them to its own request by position.
    #[tokio::test]
    async fn a_batch_resolves_in_request_order() {
        let (mut client, archive) = serve().await;
        let requested = vec![key("temp", &[]), key("flow", &[]), key("pressure", &[])];

        let ids = register(&mut client, requested.clone())
            .await
            .expect("register");

        assert_eq!(ids.len(), 3);
        for (position, series) in requested.iter().enumerate() {
            let resolved = archive
                .registry()
                .series(ids[position])
                .expect("id resolves back");
            assert_eq!(
                resolved.name(),
                series.name,
                "id at position {position} names the wrong series"
            );
        }
    }

    /// §10: registration is idempotent, which is what makes a client's cached id
    /// valid for the life of the archive.
    #[tokio::test]
    async fn registering_the_same_series_twice_returns_the_same_id() {
        let (mut client, _archive) = serve().await;
        let series = vec![key("temp", &[("plant", "a")])];

        let first = register(&mut client, series.clone()).await.unwrap();
        let second = register(&mut client, series).await.unwrap();

        assert_eq!(first, second);
    }

    /// §10: label order on the wire must not change which series is named. The
    /// Protobuf map has no order, so the server has to canonicalise.
    #[tokio::test]
    async fn label_order_does_not_change_the_series() {
        let (mut client, _archive) = serve().await;

        let forwards = register(
            &mut client,
            vec![key("temp", &[("plant", "a"), ("line", "1")])],
        )
        .await
        .unwrap();
        let backwards = register(
            &mut client,
            vec![key("temp", &[("line", "1"), ("plant", "a")])],
        )
        .await
        .unwrap();

        assert_eq!(forwards, backwards);
    }

    /// §10: labels are part of the identity, so differing labels are different
    /// series — otherwise two parallel series would collapse into one logical row.
    #[tokio::test]
    async fn differing_labels_get_different_ids() {
        let (mut client, _archive) = serve().await;

        let ids = register(
            &mut client,
            vec![
                key("temp", &[("plant", "a")]),
                key("temp", &[("plant", "b")]),
            ],
        )
        .await
        .unwrap();

        assert_ne!(ids[0], ids[1]);
    }

    /// §4: the id is durable before the client sees it, so a cached id survives a
    /// crash. Checked by reopening the archive over the same store.
    #[tokio::test]
    async fn a_registered_id_survives_a_restart() {
        let storage = Arc::new(InMemoryStorage::new());
        let config = ArchiveConfig {
            wal: WalConfig {
                segment_bytes: 64 * 1024,
                max_commit_delay: Duration::ZERO,
            },
            ..ArchiveConfig::default()
        };

        let issued = {
            let archive = Arc::new(
                Archive::open(
                    Arc::clone(&storage) as Arc<_>,
                    &config,
                    Arc::new(TestClock::new(1_000)),
                )
                .unwrap(),
            );
            let service = HetidaFlightService::new(archive);
            let response = service
                .do_action(Request::new(Action {
                    r#type: REGISTER_ACTION.to_owned(),
                    body: RegisterRequest {
                        series: vec![key("temp", &[("plant", "a")])],
                    }
                    .encode_to_vec()
                    .into(),
                }))
                .await
                .expect("register");

            let mut results = response.into_inner();
            let first = futures::TryStreamExt::try_next(&mut results)
                .await
                .unwrap()
                .unwrap();
            RegisterResponse::decode(first.body.as_ref())
                .unwrap()
                .metric_ids
        };

        let reopened = Archive::open(
            Arc::clone(&storage) as Arc<_>,
            &config,
            Arc::new(TestClock::new(2_000)),
        )
        .expect("reopen");

        assert_eq!(
            reopened.registry().resolve(&SeriesKey::new(
                "temp",
                [("plant".to_owned(), "a".to_owned())]
            )),
            issued.first().copied(),
            "an id a client cached must still resolve after a restart"
        );
    }

    #[tokio::test]
    async fn an_unknown_action_is_unimplemented_not_a_crash() {
        let (mut client, _archive) = serve().await;

        let status = client
            .do_action(Action {
                r#type: "polish-the-silverware".to_owned(),
                body: Vec::new().into(),
            })
            .await
            .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn a_malformed_body_is_invalid_argument() {
        let (mut client, _archive) = serve().await;

        let status = client
            .do_action(Action {
                r#type: REGISTER_ACTION.to_owned(),
                body: b"not a protobuf message at all".to_vec().into(),
            })
            .await
            .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// An empty request is a client bug rather than a request for nothing, so it
    /// is refused instead of answered with an empty list.
    #[tokio::test]
    async fn an_empty_request_is_refused() {
        let (mut client, _archive) = serve().await;

        let status = register(&mut client, Vec::new())
            .await
            .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn an_empty_series_name_is_refused() {
        let (mut client, _archive) = serve().await;

        let status = register(&mut client, vec![key("", &[("plant", "a")])])
            .await
            .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// A generic Flight client discovers what a server serves, so the action has
    /// to be listed rather than only documented.
    #[tokio::test]
    async fn the_register_action_is_discoverable() {
        let (mut client, _archive) = serve().await;

        let mut listed = client.list_actions(Empty {}).await.unwrap().into_inner();
        let first = futures::TryStreamExt::try_next(&mut listed)
            .await
            .unwrap()
            .expect("one action");

        assert_eq!(first.r#type, REGISTER_ACTION);
        assert!(!first.description.is_empty());
    }

    /// §4, §5: the round trip — a client writes batches and gets one
    /// acknowledgement per batch, and the rows are then in the archive.
    #[tokio::test]
    async fn writing_over_flight_ingests_and_acknowledges() {
        let (mut client, archive) = serve().await;
        let ids = register(&mut client, vec![key("temp", &[])]).await.unwrap();

        let acks = write(
            &mut client,
            "readings",
            vec![client_batch(&[(ids[0], 1_000, 1.5)])],
        )
        .await
        .expect("do_put");

        assert_eq!(acks, 1, "one acknowledgement per batch (§4)");
        assert_eq!(archive.table("readings").expect("readings").num_rows(), 1);
    }

    #[tokio::test]
    async fn several_batches_in_one_stream_are_all_acknowledged() {
        let (mut client, archive) = serve().await;
        let ids = register(&mut client, vec![key("temp", &[])]).await.unwrap();

        let acks = write(
            &mut client,
            "readings",
            (0..4)
                .map(|row| client_batch(&[(ids[0], 1_000 + row * 1_000, row as f64)]))
                .collect(),
        )
        .await
        .expect("do_put");

        assert_eq!(acks, 4);
        assert_eq!(archive.table("readings").unwrap().num_rows(), 4);
    }

    /// §4: acked means durable. What a client was told survived is in the log, so
    /// reopening the archive over the same store finds it.
    #[tokio::test]
    async fn acknowledged_rows_are_durable() {
        let storage = Arc::new(InMemoryStorage::new());
        let config = ArchiveConfig {
            wal: WalConfig {
                segment_bytes: 64 * 1024,
                max_commit_delay: Duration::ZERO,
            },
            ..ArchiveConfig::default()
        };

        {
            let archive = Arc::new(
                Archive::open(
                    Arc::clone(&storage) as Arc<_>,
                    &config,
                    Arc::new(TestClock::new(1_000)),
                )
                .unwrap(),
            );
            archive
                .ingest("readings", &client_batch(&[(1, 1_000, 1.5)]))
                .expect("ingest");
        }

        let reopened = Archive::open(
            Arc::clone(&storage) as Arc<_>,
            &config,
            Arc::new(TestClock::new(2_000)),
        )
        .unwrap();

        assert_eq!(reopened.recovered().data_records, 1);
        assert_eq!(reopened.table("readings").unwrap().num_rows(), 1);
    }

    /// §5: the table comes from the descriptor path. Without one the server
    /// cannot know where to write, and guessing would be worse than refusing.
    #[tokio::test]
    async fn a_stream_without_a_table_is_refused() {
        let (mut client, _archive) = serve().await;
        let batch = client_batch(&[(1, 1_000, 1.5)]);

        // A stream carrying data but no descriptor.
        let (encoded, _) = encode_without_descriptor(&batch);
        let status = client
            .do_put(stream::iter(encoded))
            .await
            .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// §6: a type change is refused as INVALID_ARGUMENT — retrying will never
    /// help, and the client needs to know that rather than backing off.
    #[tokio::test]
    async fn a_conflicting_type_is_invalid_argument() {
        let (mut client, _archive) = serve().await;
        write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, 1_000, 1.5)])],
        )
        .await
        .unwrap();

        let conflicting = arrow::array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
                Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
                Field::new("value", DataType::Int64, true),
            ])),
            vec![
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                    1_000i64,
                ])),
                Arc::new(arrow::array::UInt64Array::from(vec![1u64])),
                Arc::new(arrow::array::Int64Array::from(vec![5i64])),
            ],
        )
        .unwrap();

        let status = write(&mut client, "readings", vec![conflicting])
            .await
            .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// §7: past the memory cap a client gets RESOURCE_EXHAUSTED — a cause it can
    /// back off on, rather than a timeout with no explanation.
    #[tokio::test]
    async fn past_the_memory_cap_the_client_gets_resource_exhausted() {
        let storage = Arc::new(InMemoryStorage::new());
        let archive = Arc::new(
            Archive::open(
                storage,
                &ArchiveConfig {
                    max_memtable_bytes: 1,
                    wal: WalConfig {
                        segment_bytes: 64 * 1024,
                        max_commit_delay: Duration::ZERO,
                    },
                    ..ArchiveConfig::default()
                },
                Arc::new(TestClock::new(1_000)),
            )
            .unwrap(),
        );
        let (mut client, _) = serve_archive(Arc::clone(&archive)).await;

        // The first write fills the memtable; the second meets the cap.
        write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, 1_000, 1.5)])],
        )
        .await
        .expect("first write");
        let status = write(
            &mut client,
            "readings",
            vec![client_batch(&[(2, 2_000, 2.5)])],
        )
        .await
        .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }

    /// §5: an endpoint that does not exist yet must say so, not drop the
    /// connection — a generic client expects to be told.
    #[tokio::test]
    async fn unserved_endpoints_report_unimplemented() {
        let (mut client, _archive) = serve().await;

        assert_eq!(
            client
                .do_get(Ticket {
                    ticket: Vec::new().into()
                })
                .await
                .expect_err("not yet served")
                .code(),
            tonic::Code::Unimplemented
        );
        assert_eq!(
            client
                .get_flight_info(FlightDescriptor::new_path(vec!["readings".to_owned()]))
                .await
                .expect_err("not yet served")
                .code(),
            tonic::Code::Unimplemented
        );
    }

    /// Registration is off the hot path but still concurrent: many clients start
    /// at once, and two resolving the same series must not split it (§10).
    #[tokio::test]
    async fn concurrent_clients_resolving_one_series_agree() {
        let (client, _archive) = serve().await;

        let calls = (0..8).map(|_| {
            let mut client = client.clone();
            async move { register(&mut client, vec![key("temp", &[("plant", "a")])]).await }
        });

        let results = futures::future::join_all(calls).await;
        let ids: Vec<u64> = results
            .into_iter()
            .map(|result| result.expect("register")[0])
            .collect();

        assert!(
            ids.iter().all(|id| *id == ids[0]),
            "one series was split across ids: {ids:?}"
        );
    }
}
