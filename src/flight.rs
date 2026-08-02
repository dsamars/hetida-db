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
//!
//! # Reading
//!
//! A read is a [`Query`] — SQL plus an optional observation point (§1) — carried
//! as the `cmd` of a `FlightDescriptor` or as a `Ticket`. Both forms hold the
//! same bytes, so the standard `get_flight_info` → `do_get` flow works without
//! the server having to remember anything between the two calls: the ticket it
//! hands back is the request it was given. A client that already knows what it
//! wants may skip straight to `do_get`.
//!
//! Results stream. `do_get` writes batches as DataFusion produces them, and
//! never collects an answer that has no reason to fit in memory.
//!
//! # Authorization
//!
//! [`BearerInterceptor`] validates the token once per call and attaches a
//! [`Principal`], which `do_put` checks against the table its descriptor names
//! and `do_get` against every table its SQL reads (§9). `handshake` is the one
//! call that needs no token, since it is where one is obtained.
//!
//! A service built with [`HetidaFlightService::new`] enforces nothing: it is for
//! a trusted network and for tests, and the binary refuses to start in that mode
//! unless an operator says so explicitly. [`HetidaFlightService::with_auth`] is
//! the configuration §9 describes.

use std::pin::Pin;
use std::sync::Arc;

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::archive::{Archive, IngestError};
use crate::auth::{AuthError, Principal, TokenIssuer, bearer_token};
use crate::proto::{Query, RegisterRequest, RegisterResponse};
use crate::query::{QueryEngine, QueryError};
use crate::registry::SeriesKey;
use crate::storage::Storage;

/// Action type resolving series identities to ids (§10).
pub const REGISTER_ACTION: &str = "register";

/// The metadata key a bearer token travels in (§9).
pub const AUTHORIZATION: &str = "authorization";

/// Arrow Flight service over one archive.
#[derive(Debug)]
pub struct HetidaFlightService {
    archive: Arc<Archive>,
    engine: Arc<QueryEngine>,
    issuer: Option<Arc<TokenIssuer>>,
}

impl HetidaFlightService {
    /// Serves `archive` with no authorization at all.
    ///
    /// Every call is granted everything. Only for a trusted network and for
    /// tests — [`Self::with_auth`] is what §9 describes, and the binary refuses
    /// to start this way unless an operator asks for it by name.
    pub fn new(archive: Arc<Archive>, storage: Arc<dyn Storage>) -> Self {
        Self {
            engine: Arc::new(QueryEngine::new(Arc::clone(&archive), storage)),
            archive,
            issuer: None,
        }
    }

    /// Serves `archive` with per-table authorization against `issuer` (§9).
    pub fn with_auth(
        archive: Arc<Archive>,
        storage: Arc<dyn Storage>,
        issuer: Arc<TokenIssuer>,
    ) -> Self {
        Self {
            issuer: Some(issuer),
            ..Self::new(archive, storage)
        }
    }

    /// The principal a call was made by, or `None` when nothing is enforced.
    ///
    /// The token was already validated by [`BearerInterceptor`], once for the
    /// whole call (§9). This only reads what it left behind — nothing here
    /// touches a credential, which is what keeps validation off the per-message
    /// path of a `do_put` stream.
    fn principal<T>(&self, request: &Request<T>) -> Result<Option<Principal>, Status> {
        if self.issuer.is_none() {
            return Ok(None);
        }
        request
            .extensions()
            .get::<Principal>()
            .cloned()
            .map(Some)
            .ok_or_else(|| auth_status(&AuthError::Unauthenticated))
    }

    /// The archive this service serves.
    pub fn archive(&self) -> &Arc<Archive> {
        &self.archive
    }

    /// Refuses unless `principal` may read every table the query reads (§9).
    ///
    /// From the planned query, not from its text: a table reached through a
    /// view, a subquery or a join is a table the answer depends on, and a grant
    /// check that only read the `FROM` clause would miss it.
    async fn authorize_read(
        &self,
        query: &Query,
        principal: Option<&Principal>,
    ) -> Result<(), Status> {
        let Some(principal) = principal else {
            return Ok(());
        };
        let tables = self
            .engine
            .tables_of(&query.sql, query.as_of_micros)
            .await
            .map_err(query_status)?;

        principal
            .may_read_all(tables.iter().map(String::as_str))
            .map_err(|error| auth_status(&error))
    }

    /// The schema a query answers with, planned but not run (§5).
    async fn schema_for(&self, query: &Query) -> Result<arrow::datatypes::SchemaRef, Status> {
        self.engine
            .schema_of(&query.sql, query.as_of_micros)
            .await
            .map_err(query_status)
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
/// The distinctions matter, because each tells the client to do something
/// different:
///
/// - `RESOURCE_EXHAUSTED` — back off and retry the same batch (§7).
/// - `INVALID_ARGUMENT` — the batch is wrong in itself and will never be
///   accepted, whatever the server's state (§6).
/// - `FAILED_PRECONDITION` — the batch is well formed but has fallen behind the
///   retention cutoff (§8). Backing off makes it worse: the cutoff only moves
///   further forward, so this is a retry the client must not make.
fn ingest_status(error: IngestError) -> Status {
    match error {
        IngestError::Exhausted { .. } => Status::resource_exhausted(error.to_string()),
        IngestError::Catalogue(_) | IngestError::Partition(_) => {
            Status::invalid_argument(error.to_string())
        }
        IngestError::Expired { .. } => Status::failed_precondition(error.to_string()),
        IngestError::Stamp(_) | IngestError::Wal(_) | IngestError::Table(_) => {
            Status::internal(error.to_string())
        }
    }
}

/// The query a `cmd` or a `Ticket` carries (§5).
fn query_from(payload: &[u8]) -> Result<Query, Status> {
    let query = Query::decode(payload).map_err(|error| {
        Status::invalid_argument(format!(
            "expected a hetida.v1.Query in the descriptor cmd or ticket: {error}"
        ))
    })?;
    if query.sql.trim().is_empty() {
        return Err(Status::invalid_argument("a query needs SQL"));
    }
    Ok(query)
}

/// Maps a query failure onto the status a client should see.
///
/// `INVALID_ARGUMENT` throughout: the SQL is the client's, and every way it can
/// fail — a syntax error, an unknown table, a type it cannot compare — is a
/// question the server will never answer, however often it is asked.
fn query_status(error: QueryError) -> Status {
    Status::invalid_argument(error.to_string())
}

/// Maps an authorization failure onto the status a client should see (§9).
fn auth_status(error: &AuthError) -> Status {
    match error {
        AuthError::Unauthenticated | AuthError::BadCredentials => {
            Status::unauthenticated(error.to_string())
        }
        AuthError::Forbidden { .. } => Status::permission_denied(error.to_string()),
    }
}

/// Validates the bearer token once per call and attaches the principal (§9).
///
/// A `tonic` interceptor runs once per call, before the handler and before a
/// single message is read. That is the whole point: one `do_put` stream carries
/// thousands of `RecordBatch`es behind one authorization, and validating a token
/// per message would put it in the ingestion hot path.
///
/// A call with no valid token is not refused *here*, because `handshake` is a
/// call and has no token to present. It simply arrives with no principal
/// attached, and every handler that needs one refuses it.
#[derive(Debug, Clone)]
pub struct BearerInterceptor {
    issuer: Arc<TokenIssuer>,
}

impl BearerInterceptor {
    /// Validates against `issuer`.
    pub fn new(issuer: Arc<TokenIssuer>) -> Self {
        Self { issuer }
    }
}

impl tonic::service::Interceptor for BearerInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let token = request
            .metadata()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_token)
            .map(ToOwned::to_owned);

        if let Some(principal) = token.and_then(|token| self.issuer.validate(&token).ok()) {
            request.extensions_mut().insert(principal);
        }
        Ok(request)
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

    /// Exchanges credentials for a bearer token (§9).
    ///
    /// The payload is `user:password`, as Flight's handshake conventionally
    /// carries it, and the token comes back both in the response payload and in
    /// the `authorization` metadata so either style of client can pick it up.
    /// This is the one call that needs no token.
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let Some(issuer) = self.issuer.clone() else {
            return Err(Status::unimplemented(
                "this server enforces no authorization, so there is no token to issue (§9)",
            ));
        };

        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("handshake received no credentials"))?;

        let payload = String::from_utf8(first.payload.to_vec()).map_err(|_| {
            Status::invalid_argument("credentials must be `user:password` in UTF-8")
        })?;
        let (user, password) = payload
            .split_once(':')
            .ok_or_else(|| Status::invalid_argument("credentials must be `user:password`"))?;

        let token = issuer
            .handshake(user, password)
            .map_err(|error| auth_status(&error))?;

        let mut response = Response::new(one(HandshakeResponse {
            protocol_version: 0,
            payload: token.clone().into_bytes().into(),
        }) as Self::HandshakeStream);
        if let (Ok(key), Ok(value)) = (AUTHORIZATION.parse(), format!("Bearer {token}").parse()) {
            let key: tonic::metadata::AsciiMetadataKey = key;
            response.metadata_mut().insert(key, value);
        }
        Ok(response)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "list_flights arrives with the query engine (§5)",
        ))
    }

    /// Describes a query and hands back the ticket that runs it (§5).
    ///
    /// The ticket *is* the request: the same `cmd` bytes travel back out, so no
    /// state has to survive between this call and the `do_get` that follows —
    /// which also means a ticket does not expire, and a client may keep one.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let principal = self.principal(&request)?;
        let descriptor = request.into_inner();
        let query = query_from(&descriptor.cmd)?;
        // A schema describes the data, so it needs the same grant the rows do.
        self.authorize_read(&query, principal.as_ref()).await?;
        let schema = self.schema_for(&query).await?;

        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|error| Status::internal(format!("could not encode the schema: {error}")))?
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket {
                ticket: descriptor.cmd.clone(),
            }))
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "long-running queries are not part of v1 (§5)",
        ))
    }

    /// The schema a query would answer with, without running it (§5).
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let principal = self.principal(&request)?;
        let query = query_from(&request.into_inner().cmd)?;
        self.authorize_read(&query, principal.as_ref()).await?;
        let schema = self.schema_for(&query).await?;

        SchemaAsIpc::new(schema.as_ref(), &IpcWriteOptions::default())
            .try_into()
            .map(Response::new)
            .map_err(|error| Status::internal(format!("could not encode the schema: {error}")))
    }

    /// Runs a query and streams its result as Arrow IPC (§1, §5).
    ///
    /// Streamed, not collected: batches go out as DataFusion produces them, so
    /// the server never holds an answer whose size the client chose.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let principal = self.principal(&request)?;
        let query = query_from(&request.into_inner().ticket)?;
        self.authorize_read(&query, principal.as_ref()).await?;

        let results = self
            .engine
            .stream(&query.sql, query.as_of_micros)
            .await
            .map_err(query_status)?;

        let batches = results.map_err(|error| FlightError::ExternalError(Box::new(error)));
        let encoded = FlightDataEncoderBuilder::new()
            .build(batches)
            .map_err(|error| Status::internal(error.to_string()));

        Ok(Response::new(Box::pin(encoded)))
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
        let principal = self.principal(&request)?;
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

        // §9: once, for the whole stream. The batches behind this one check are
        // unbounded in number, which is exactly why it does not repeat.
        if let Some(principal) = &principal {
            principal.may_write(&table).map_err(|e| auth_status(&e))?;
        }

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
                Arc::clone(&storage) as Arc<_>,
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
        serve_archive(archive, storage).await
    }

    /// Serves an archive the caller built, for tests that need a specific config.
    async fn serve_archive(
        archive: Arc<Archive>,
        storage: Arc<InMemoryStorage>,
    ) -> (FlightServiceClient<tonic::transport::Channel>, Arc<Archive>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let service = HetidaFlightService::new(Arc::clone(&archive), storage as Arc<_>);

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
            let service = HetidaFlightService::new(archive, Arc::clone(&storage) as Arc<_>);
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
                Arc::clone(&storage) as Arc<_>,
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
        let (mut client, _) = serve_archive(Arc::clone(&archive), storage).await;

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

    /// §8: data already outside the retention window is refused with an
    /// explicit Flight error, not written and reaped moments later.
    /// `FAILED_PRECONDITION` rather than `RESOURCE_EXHAUSTED`: backing off makes
    /// it worse, because the cutoff only moves further forward.
    #[tokio::test]
    async fn expired_data_is_refused_with_an_explicit_error() {
        const AUGUST: i64 = 1_785_681_432_000_000;
        const JUNE: i64 = 1_781_481_600_000_000;
        const DAY: i64 = 86_400_000_000;

        let storage = Arc::new(InMemoryStorage::new());
        let archive = Arc::new(
            Archive::open(
                Arc::clone(&storage) as Arc<_>,
                &ArchiveConfig {
                    tables: vec![(
                        "readings".to_owned(),
                        crate::catalog::TableConfig {
                            retention: Some(30 * DAY),
                            ..crate::catalog::TableConfig::default()
                        },
                    )],
                    wal: WalConfig {
                        segment_bytes: 64 * 1024,
                        max_commit_delay: Duration::ZERO,
                    },
                    ..ArchiveConfig::default()
                },
                Arc::new(TestClock::new(AUGUST)),
            )
            .unwrap(),
        );
        let (mut client, _) = serve_archive(Arc::clone(&archive), storage).await;

        // A live partition is accepted.
        write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, AUGUST, 1.5)])],
        )
        .await
        .expect("august is live");

        let status = write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, JUNE, 2.5)])],
        )
        .await
        .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            status.message().contains("retention"),
            "the client must be told why: {}",
            status.message()
        );
    }

    // ---- reading (§1, §5) ----

    /// Runs a query over Flight and returns the rows it answered with.
    async fn read(
        client: &mut FlightServiceClient<tonic::transport::Channel>,
        sql: &str,
        as_of: Option<i64>,
    ) -> Result<Vec<arrow::array::RecordBatch>, Status> {
        let ticket = Ticket {
            ticket: Query {
                sql: sql.to_owned(),
                as_of_micros: as_of,
            }
            .encode_to_vec()
            .into(),
        };
        let stream = client.do_get(ticket).await?.into_inner();
        FlightRecordBatchStream::new_from_flight_data(stream.map_err(FlightError::from))
            .try_collect()
            .await
            .map_err(|error| Status::internal(error.to_string()))
    }

    /// §5: the round trip a client actually makes — write, then read the same
    /// rows back over `do_get`.
    #[tokio::test]
    async fn written_rows_come_back_over_do_get() {
        let (mut client, _archive) = serve().await;
        write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, 1_000, 1.5), (2, 2_000, 2.5)])],
        )
        .await
        .expect("do_put");

        let batches = read(&mut client, "SELECT * FROM readings", None)
            .await
            .expect("do_get");

        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            2,
            "both rows must come back"
        );
    }

    /// §1: the observation point travels in the ticket, and answers with the
    /// version current at it.
    #[tokio::test]
    async fn the_ticket_carries_the_observation_point() {
        let storage = Arc::new(InMemoryStorage::new());
        let clock = Arc::new(TestClock::new(1_000));
        let archive = Arc::new(
            Archive::open(
                Arc::clone(&storage) as Arc<_>,
                &ArchiveConfig {
                    wal: WalConfig {
                        segment_bytes: 64 * 1024,
                        max_commit_delay: Duration::ZERO,
                    },
                    ..ArchiveConfig::default()
                },
                Arc::clone(&clock) as Arc<_>,
            )
            .unwrap(),
        );
        let (mut client, _) = serve_archive(Arc::clone(&archive), storage).await;

        write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, 500, 1.5)])],
        )
        .await
        .unwrap();
        clock.set(9_000);
        write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, 500, 9.9)])],
        )
        .await
        .unwrap();

        let earlier = read(&mut client, "SELECT * FROM readings", Some(1_000))
            .await
            .unwrap();
        let now = read(&mut client, "SELECT * FROM readings", None)
            .await
            .unwrap();

        let value = |batches: &[arrow::array::RecordBatch]| -> f64 {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name("value")
                        .and_then(|c| c.as_any().downcast_ref::<arrow::array::Float64Array>())
                        .map(|values| values.values().to_vec())
                        .unwrap_or_default()
                })
                .next()
                .expect("a value")
        };

        assert_eq!(value(&earlier), 1.5, "the earlier point sees the original");
        assert_eq!(value(&now), 9.9, "and now sees the correction");
    }

    /// §5: the standard flow. `get_flight_info` describes the query and hands
    /// back a ticket, and that ticket runs it — so a generic Flight client that
    /// knows nothing about this server can follow it.
    #[tokio::test]
    async fn get_flight_info_hands_back_a_ticket_that_works() {
        let (mut client, _archive) = serve().await;
        write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, 1_000, 1.5)])],
        )
        .await
        .unwrap();

        let info = client
            .get_flight_info(FlightDescriptor::new_cmd(
                Query {
                    sql: "SELECT * FROM readings".to_owned(),
                    as_of_micros: None,
                }
                .encode_to_vec(),
            ))
            .await
            .expect("get_flight_info")
            .into_inner();

        let ticket = info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.clone())
            .expect("an endpoint with a ticket");

        let stream = client.do_get(ticket).await.expect("do_get").into_inner();
        let batches: Vec<arrow::array::RecordBatch> =
            FlightRecordBatchStream::new_from_flight_data(stream.map_err(FlightError::from))
                .try_collect()
                .await
                .expect("rows");

        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert!(!info.schema.is_empty(), "the info must describe the schema");
    }

    /// §5: `get_schema` answers without running the query, so a client can
    /// prepare for an answer it has not asked for yet.
    #[tokio::test]
    async fn get_schema_describes_a_query_without_running_it() {
        let (mut client, _archive) = serve().await;
        write(
            &mut client,
            "readings",
            vec![client_batch(&[(1, 1_000, 1.5)])],
        )
        .await
        .unwrap();

        let result = client
            .get_schema(FlightDescriptor::new_cmd(
                Query {
                    sql: "SELECT * FROM readings".to_owned(),
                    as_of_micros: None,
                }
                .encode_to_vec(),
            ))
            .await
            .expect("get_schema")
            .into_inner();

        let schema = arrow::datatypes::Schema::try_from(&result).expect("a schema");
        assert!(schema.field_with_name("value").is_ok());
        assert!(
            schema.field_with_name(crate::schema::SEQ_COLUMN).is_err(),
            "the resolved view hides the system columns (§1)"
        );
    }

    /// A ticket that is not a `Query` is a client bug, and saying so beats
    /// failing somewhere inside the planner.
    #[tokio::test]
    async fn a_malformed_ticket_is_invalid_argument() {
        let (mut client, _archive) = serve().await;

        let status = client
            .do_get(Ticket {
                ticket: b"not a protobuf message at all".to_vec().into(),
            })
            .await
            .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// SQL the server will never answer is `INVALID_ARGUMENT`, not an internal
    /// error: retrying it changes nothing.
    #[tokio::test]
    async fn a_query_against_an_unknown_table_is_invalid_argument() {
        let (mut client, _archive) = serve().await;

        let status = read(&mut client, "SELECT * FROM absent", None)
            .await
            .expect_err("must be refused");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// §5: what is still unserved must say so rather than drop the connection.
    #[tokio::test]
    async fn unserved_endpoints_report_unimplemented() {
        let (mut client, _archive) = serve().await;

        assert_eq!(
            client
                .handshake(stream::iter(vec![HandshakeRequest::default()]))
                .await
                .expect_err("not yet served")
                .code(),
            tonic::Code::Unimplemented
        );
    }

    // ---- authentication and authorization (§9) ----

    /// Serves an archive with per-table grants behind a bearer token.
    async fn serve_with_auth(
        credentials: Arc<crate::auth::StaticCredentials>,
    ) -> FlightServiceClient<tonic::transport::Channel> {
        use crate::auth::{DEFAULT_TOKEN_TTL, TestTokens};

        let storage = Arc::new(InMemoryStorage::new());
        let archive = Arc::new(
            Archive::open(
                Arc::clone(&storage) as Arc<_>,
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
        let issuer = Arc::new(TokenIssuer::new(
            credentials,
            Arc::new(TestTokens::default()),
            Arc::new(TestClock::new(1_000)),
            DEFAULT_TOKEN_TTL,
        ));
        let service =
            HetidaFlightService::with_auth(archive, storage as Arc<_>, Arc::clone(&issuer));

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
        loop {
            match tonic::transport::Endpoint::from_shared(endpoint.clone())
                .expect("endpoint")
                .connect()
                .await
            {
                Ok(channel) => break FlightServiceClient::new(channel),
                Err(_) => tokio::task::yield_now().await,
            }
        }
    }

    fn credentials() -> Arc<crate::auth::StaticCredentials> {
        use crate::auth::{Grants, StaticCredentials, TableSet};

        Arc::new(
            StaticCredentials::new()
                .with_user("writer", "s3cret", Grants::all())
                .with_user("reader", "hunter2", Grants::read_only(["readings"]))
                .with_user(
                    "events-only",
                    "pw",
                    Grants {
                        read: TableSet::named(["events"]),
                        write: TableSet::named(["events"]),
                    },
                ),
        )
    }

    /// Exchanges credentials for a token over the handshake.
    async fn handshake(
        client: &mut FlightServiceClient<tonic::transport::Channel>,
        user: &str,
        password: &str,
    ) -> Result<String, Status> {
        let request = HandshakeRequest {
            protocol_version: 0,
            payload: format!("{user}:{password}").into_bytes().into(),
        };
        let mut responses = client
            .handshake(stream::iter(vec![request]))
            .await?
            .into_inner();
        let first = futures::TryStreamExt::try_next(&mut responses)
            .await?
            .ok_or_else(|| Status::internal("no handshake response"))?;

        String::from_utf8(first.payload.to_vec())
            .map_err(|_| Status::internal("token is not UTF-8"))
    }

    /// Attaches a bearer token to a request, as a client does on every call.
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

    /// A `do_get` carrying a token, or none.
    async fn read_as(
        client: &mut FlightServiceClient<tonic::transport::Channel>,
        sql: &str,
        token: Option<&str>,
    ) -> Result<usize, Status> {
        let ticket = Ticket {
            ticket: Query {
                sql: sql.to_owned(),
                as_of_micros: None,
            }
            .encode_to_vec()
            .into(),
        };
        let request = match token {
            Some(token) => bearing(ticket, token),
            None => Request::new(ticket),
        };

        let stream = client.do_get(request).await?.into_inner();
        let batches: Vec<arrow::array::RecordBatch> =
            FlightRecordBatchStream::new_from_flight_data(stream.map_err(FlightError::from))
                .try_collect()
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
        Ok(batches.iter().map(|b| b.num_rows()).sum())
    }

    /// A `do_put` carrying a token, or none.
    async fn write_as(
        client: &mut FlightServiceClient<tonic::transport::Channel>,
        table: &str,
        token: Option<&str>,
    ) -> Result<usize, Status> {
        let encoded: Vec<FlightData> = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![table.to_owned()])))
            .build(stream::iter(vec![Ok(client_batch(&[(1, 1_000, 1.5)]))]))
            .try_collect()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        let outgoing = stream::iter(encoded);
        let request = match token {
            Some(token) => bearing(outgoing, token),
            None => Request::new(outgoing),
        };

        let mut acks = client.do_put(request).await?.into_inner();
        let mut count = 0;
        while futures::TryStreamExt::try_next(&mut acks).await?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    /// §9: a handshake exchanges credentials for a bearer token, and that token
    /// is what admits every later call.
    #[tokio::test]
    async fn a_handshake_yields_a_token_that_admits_calls() {
        let mut client = serve_with_auth(credentials()).await;

        let token = handshake(&mut client, "writer", "s3cret")
            .await
            .expect("accepted");

        assert_eq!(
            write_as(&mut client, "readings", Some(&token))
                .await
                .expect("the token must admit the write"),
            1
        );
    }

    /// §9: an unauthenticated call is refused, on both directions.
    #[tokio::test]
    async fn calls_without_a_token_are_refused() {
        let mut client = serve_with_auth(credentials()).await;

        assert_eq!(
            write_as(&mut client, "readings", None)
                .await
                .expect_err("must be refused")
                .code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            read_as(&mut client, "SELECT 1", None)
                .await
                .expect_err("must be refused")
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    #[tokio::test]
    async fn an_invented_token_is_refused() {
        let mut client = serve_with_auth(credentials()).await;

        assert_eq!(
            write_as(&mut client, "readings", Some("deadbeef"))
                .await
                .expect_err("must be refused")
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    #[tokio::test]
    async fn wrong_credentials_do_not_yield_a_token() {
        let mut client = serve_with_auth(credentials()).await;

        assert_eq!(
            handshake(&mut client, "writer", "wrong")
                .await
                .expect_err("must be refused")
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    /// §9: the grant is per table and per direction, checked against the table
    /// the `do_put` descriptor names.
    #[tokio::test]
    async fn a_write_is_refused_on_an_ungranted_table() {
        let mut client = serve_with_auth(credentials()).await;
        let token = handshake(&mut client, "events-only", "pw").await.unwrap();

        assert_eq!(
            write_as(&mut client, "events", Some(&token))
                .await
                .expect("granted"),
            1
        );
        assert_eq!(
            write_as(&mut client, "readings", Some(&token))
                .await
                .expect_err("must be refused")
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    /// §9: a reader may read what it was granted and write nothing.
    #[tokio::test]
    async fn a_reader_may_not_write() {
        let mut client = serve_with_auth(credentials()).await;
        let writer = handshake(&mut client, "writer", "s3cret").await.unwrap();
        write_as(&mut client, "readings", Some(&writer))
            .await
            .unwrap();

        let reader = handshake(&mut client, "reader", "hunter2").await.unwrap();

        assert_eq!(
            read_as(&mut client, "SELECT * FROM readings", Some(&reader))
                .await
                .expect("granted"),
            1
        );
        assert_eq!(
            write_as(&mut client, "readings", Some(&reader))
                .await
                .expect_err("must be refused")
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    /// §9: the read grant is checked against the tables the *plan* reads, not
    /// against the text — so a table reached through a view or a join is caught.
    #[tokio::test]
    async fn a_read_is_refused_on_a_table_reached_indirectly() {
        let mut client = serve_with_auth(credentials()).await;
        let writer = handshake(&mut client, "writer", "s3cret").await.unwrap();
        write_as(&mut client, "readings", Some(&writer))
            .await
            .unwrap();
        write_as(&mut client, "events", Some(&writer))
            .await
            .unwrap();

        let reader = handshake(&mut client, "reader", "hunter2").await.unwrap();

        // Granted directly.
        assert!(
            read_as(&mut client, "SELECT * FROM readings", Some(&reader))
                .await
                .is_ok()
        );
        // The history view of the same table is the same grant.
        assert!(
            read_as(
                &mut client,
                "SELECT * FROM \"readings$history\"",
                Some(&reader)
            )
            .await
            .is_ok(),
            "a view of a granted table is not a separate object"
        );
        // And a join that reaches an ungranted one is refused whole.
        assert_eq!(
            read_as(
                &mut client,
                "SELECT * FROM readings JOIN events USING (metric_id)",
                Some(&reader),
            )
            .await
            .expect_err("must be refused")
            .code(),
            tonic::Code::PermissionDenied,
        );
    }

    /// §9: describing data needs the same grant as reading it, or a schema
    /// would leak a table's shape to a client that may not see its rows.
    #[tokio::test]
    async fn describing_an_ungranted_table_is_refused() {
        let mut client = serve_with_auth(credentials()).await;
        let writer = handshake(&mut client, "writer", "s3cret").await.unwrap();
        write_as(&mut client, "events", Some(&writer))
            .await
            .unwrap();

        let reader = handshake(&mut client, "reader", "hunter2").await.unwrap();
        let descriptor = FlightDescriptor::new_cmd(
            Query {
                sql: "SELECT * FROM events".to_owned(),
                as_of_micros: None,
            }
            .encode_to_vec(),
        );

        assert_eq!(
            client
                .get_schema(bearing(descriptor.clone(), &reader))
                .await
                .expect_err("must be refused")
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            client
                .get_flight_info(bearing(descriptor, &reader))
                .await
                .expect_err("must be refused")
                .code(),
            tonic::Code::PermissionDenied
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
