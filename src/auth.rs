// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Authentication and per-table authorization (§9).
//!
//! A client exchanges credentials for a bearer token once, over Flight's
//! `Handshake`, and carries it in the `authorization` metadata of every later
//! call. The token is validated **once per call**, in a `tonic` interceptor —
//! never per message. One `do_put` stream carries thousands of `RecordBatch`es
//! behind a single authorization, and checking each of them would put credential
//! validation squarely in the ingestion hot path.
//!
//! # What a principal may do
//!
//! Per-table read and write grants ([`Grants`]). Table level is the natural
//! granularity because multi-table is already given (§6): a table is what a
//! `do_put` descriptor names and what a `do_get` query selects from.
//!
//! # The credential store is pluggable
//!
//! [`CredentialStore`] is the seam. v1 ships [`StaticCredentials`], read from
//! configuration at startup; OIDC/JWT and mTLS client certificates are later
//! backends and must not be precluded — which is why authentication returns a
//! [`Principal`] rather than a password verdict, and why nothing outside this
//! module knows how a token was earned.
//!
//! # Tokens are injected, not invented
//!
//! [`TokenSource`] supplies the bytes, so production draws them from the
//! operating system's entropy and a test drives them explicitly. A token
//! generated from a clock or a counter in production would be guessable; a
//! random one in a test would make the suite non-deterministic. Expiry uses the
//! injected [`Clock`], for the same reason wall-clock time never enters this
//! crate directly.
//!
//! # Authentication is not a query cost limit
//!
//! An authenticated client can still submit a ruinous query (§5, Open
//! Questions). Nothing here pretends otherwise.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::clock::{Clock, Micros};

/// How long a token issued by a handshake stays valid, by default.
///
/// Twelve hours: long enough that a batch job started overnight does not have to
/// re-handshake mid-run, short enough that a leaked token is not a permanent
/// grant.
pub const DEFAULT_TOKEN_TTL: Micros = 12 * 3_600_000_000;

/// Bytes in a bearer token, before hex encoding.
///
/// 32 bytes — 256 bits. A token is a bearer credential with no second factor
/// behind it, so it is sized against offline guessing rather than against a rate
/// limit that may not exist.
pub const TOKEN_BYTES: usize = 32;

/// Why a call was refused (§9).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    /// No token, or one this server did not issue or has expired.
    #[error("not authenticated: present a bearer token from a Flight handshake (§9)")]
    Unauthenticated,
    /// The credentials offered do not identify anyone.
    ///
    /// Deliberately does not say which half was wrong: that would turn the
    /// handshake into an oracle for valid user names.
    #[error("those credentials were not accepted")]
    BadCredentials,
    /// Authenticated, but not granted what the call asks for.
    #[error("`{principal}` may not {action} `{table}`")]
    Forbidden {
        /// Who was refused.
        principal: String,
        /// `read` or `write`.
        action: &'static str,
        /// The table they were refused on.
        table: String,
    },
}

/// What a principal may do to which tables (§9).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Grants {
    /// Tables this principal may read.
    pub read: TableSet,
    /// Tables this principal may write.
    pub write: TableSet,
}

impl Grants {
    /// A principal granted everything, for a single-tenant deployment.
    pub fn all() -> Self {
        Self {
            read: TableSet::All,
            write: TableSet::All,
        }
    }

    /// A principal granted read on `tables` and nothing else.
    pub fn read_only<I, S>(tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            read: TableSet::named(tables),
            write: TableSet::default(),
        }
    }
}

/// Which tables a grant covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableSet {
    /// Every table, including ones that do not exist yet (§6).
    All,
    /// Exactly these.
    Named(BTreeSet<String>),
}

impl Default for TableSet {
    /// Nothing. A grant nobody wrote down is not a grant.
    fn default() -> Self {
        Self::Named(BTreeSet::new())
    }
}

impl TableSet {
    /// A set naming exactly `tables`.
    pub fn named<I, S>(tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Named(tables.into_iter().map(Into::into).collect())
    }

    /// Whether this set covers `table`.
    pub fn covers(&self, table: &str) -> bool {
        match self {
            Self::All => true,
            Self::Named(named) => named.contains(table),
        }
    }
}

/// Who a call is being made by, and what they may do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The identity, for error messages and audit.
    pub name: String,
    /// What they may do (§9).
    pub grants: Grants,
}

impl Principal {
    /// Creates a principal.
    pub fn new(name: impl Into<String>, grants: Grants) -> Self {
        Self {
            name: name.into(),
            grants,
        }
    }

    /// Refuses unless this principal may write `table` (§9).
    pub fn may_write(&self, table: &str) -> Result<(), AuthError> {
        self.check(self.grants.write.covers(table), "write", table)
    }

    /// Refuses unless this principal may read `table` (§9).
    pub fn may_read(&self, table: &str) -> Result<(), AuthError> {
        self.check(self.grants.read.covers(table), "read", table)
    }

    /// Refuses unless this principal may read every table in `tables`.
    ///
    /// A query naming several is granted only if every one of them is: a join
    /// that reaches a table the client may not read would otherwise leak it
    /// through the rows it selects.
    pub fn may_read_all<'a>(
        &self,
        tables: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), AuthError> {
        for table in tables {
            self.may_read(table)?;
        }
        Ok(())
    }

    fn check(&self, allowed: bool, action: &'static str, table: &str) -> Result<(), AuthError> {
        if allowed {
            return Ok(());
        }
        Err(AuthError::Forbidden {
            principal: self.name.clone(),
            action,
            table: table.to_owned(),
        })
    }
}

/// Where credentials are checked (§9).
///
/// The seam a later OIDC or mTLS backend replaces. It answers with a
/// [`Principal`] rather than a yes or no, so a backend that learns the grants
/// from the credential itself — a JWT claim, a certificate extension — fits
/// without changing anything above it.
pub trait CredentialStore: fmt::Debug + Send + Sync {
    /// The principal these credentials identify, if any.
    fn authenticate(&self, user: &str, password: &str) -> Option<Principal>;
}

/// [`CredentialStore`] read from configuration. The v1 backend.
#[derive(Debug, Default)]
pub struct StaticCredentials {
    users: HashMap<String, (String, Grants)>,
}

impl StaticCredentials {
    /// An empty store, which accepts nobody.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a user with a password and its grants.
    pub fn with_user(
        mut self,
        user: impl Into<String>,
        password: impl Into<String>,
        grants: Grants,
    ) -> Self {
        self.users.insert(user.into(), (password.into(), grants));
        self
    }
}

impl CredentialStore for StaticCredentials {
    fn authenticate(&self, user: &str, password: &str) -> Option<Principal> {
        let (expected, grants) = self.users.get(user)?;
        // Constant-time over the whole password, so a mismatch does not leak
        // how many leading characters were right.
        constant_time_eq(expected.as_bytes(), password.as_bytes())
            .then(|| Principal::new(user, grants.clone()))
    }
}

/// Source of the bytes a bearer token is made of.
///
/// Injected for the same reason [`Clock`] is: production must draw from the
/// operating system's entropy, and a test must be able to say what a token will
/// be. A token from a counter in production is guessable; a random one under
/// test makes the suite non-deterministic.
pub trait TokenSource: fmt::Debug + Send + Sync {
    /// Fills `bytes` with unpredictable data.
    fn fill(&self, bytes: &mut [u8]);
}

/// [`TokenSource`] drawing from the operating system. The production one.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemTokens;

impl TokenSource for SystemTokens {
    fn fill(&self, bytes: &mut [u8]) {
        // `TryRngCore` because the OS source is fallible in principle. On a
        // failure the buffer would stay zeroed, so it is turned into a refusal
        // rather than a guessable token: the caller sees a token that is never
        // issued twice, because a zeroed one is rejected below.
        if rand::TryRngCore::try_fill_bytes(&mut rand::rngs::OsRng, bytes).is_err() {
            bytes.fill(0);
        }
    }
}

/// [`TokenSource`] handing out predictable bytes, for tests only.
#[derive(Debug, Default)]
pub struct TestTokens {
    next: AtomicU64,
}

impl TokenSource for TestTokens {
    fn fill(&self, bytes: &mut [u8]) {
        // From one, not zero: an all-zero token is what a failed entropy source
        // leaves behind, and the issuer refuses it.
        let value = self.next.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        bytes.fill(0);
        for (slot, byte) in bytes.iter_mut().zip(value.to_be_bytes()) {
            *slot = byte;
        }
    }
}

/// Issues bearer tokens and validates them (§9).
///
/// Tokens are opaque and held in memory: a restart invalidates every one of
/// them, which is correct — the archive's durable state is its data, not its
/// sessions, and a client that must re-handshake after a server restart is in a
/// state it already has to handle.
#[derive(Debug)]
pub struct TokenIssuer {
    credentials: Arc<dyn CredentialStore>,
    tokens: Arc<dyn TokenSource>,
    clock: Arc<dyn Clock>,
    ttl: Micros,
    issued: DashMap<String, Session>,
}

/// One issued token's principal and expiry.
#[derive(Debug, Clone)]
struct Session {
    principal: Principal,
    expires_at: Micros,
}

impl TokenIssuer {
    /// Issues tokens for principals `credentials` accepts, valid for `ttl`.
    pub fn new(
        credentials: Arc<dyn CredentialStore>,
        tokens: Arc<dyn TokenSource>,
        clock: Arc<dyn Clock>,
        ttl: Micros,
    ) -> Self {
        Self {
            credentials,
            tokens,
            clock,
            ttl,
            issued: DashMap::new(),
        }
    }

    /// Exchanges credentials for a bearer token (§9).
    pub fn handshake(&self, user: &str, password: &str) -> Result<String, AuthError> {
        let principal = self
            .credentials
            .authenticate(user, password)
            .ok_or(AuthError::BadCredentials)?;

        let mut bytes = [0u8; TOKEN_BYTES];
        self.tokens.fill(&mut bytes);
        if bytes == [0u8; TOKEN_BYTES] {
            // The source produced nothing. Issuing a token every caller could
            // guess would be worse than refusing the handshake.
            return Err(AuthError::BadCredentials);
        }
        let token = hex(&bytes);

        self.issued.insert(
            token.clone(),
            Session {
                principal,
                expires_at: self.clock.now_micros().saturating_add(self.ttl),
            },
        );
        Ok(token)
    }

    /// The principal a token identifies, if it is still valid.
    ///
    /// An expired token is removed as it is rejected, so the map does not grow
    /// with every session that was never used again.
    pub fn validate(&self, token: &str) -> Result<Principal, AuthError> {
        let session = self
            .issued
            .get(token)
            .map(|entry| entry.value().clone())
            .ok_or(AuthError::Unauthenticated)?;

        if self.clock.now_micros() >= session.expires_at {
            self.issued.remove(token);
            return Err(AuthError::Unauthenticated);
        }
        Ok(session.principal)
    }

    /// How many tokens are currently held.
    pub fn issued_count(&self) -> usize {
        self.issued.len()
    }
}

/// The token in an `authorization: Bearer …` header, if there is one.
///
/// The scheme is compared case-insensitively, as RFC 9110 requires.
pub fn bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

/// Compares two byte strings without leaking where they differ.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    // The length is not secret — a password's length leaks through its
    // ciphertext anyway — but the contents are, so the loop never stops early.
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |differences, (a, b)| differences | (a ^ b))
        == 0
}

/// Lower-case hex, so a token is safe in an HTTP header.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut text, byte| {
        // Writing to a String cannot fail.
        let _ = write!(text, "{byte:02x}");
        text
    })
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

    use crate::clock::TestClock;

    fn store() -> Arc<StaticCredentials> {
        Arc::new(
            StaticCredentials::new()
                .with_user("writer", "s3cret", Grants::all())
                .with_user("reader", "hunter2", Grants::read_only(["readings"])),
        )
    }

    fn issuer(clock: Arc<TestClock>) -> TokenIssuer {
        TokenIssuer::new(
            store(),
            Arc::new(TestTokens::default()),
            clock,
            DEFAULT_TOKEN_TTL,
        )
    }

    // ---- authentication (§9) ----

    /// §9: a handshake exchanges credentials for a bearer token, and that token
    /// is what every later call carries.
    #[test]
    fn a_handshake_exchanges_credentials_for_a_token() {
        let issuer = issuer(Arc::new(TestClock::new(0)));

        let token = issuer.handshake("writer", "s3cret").expect("accepted");

        assert_eq!(token.len(), TOKEN_BYTES * 2, "hex of {TOKEN_BYTES} bytes");
        assert_eq!(issuer.validate(&token).unwrap().name, "writer");
    }

    #[test]
    fn wrong_credentials_are_refused() {
        let issuer = issuer(Arc::new(TestClock::new(0)));

        assert_eq!(
            issuer.handshake("writer", "wrong"),
            Err(AuthError::BadCredentials)
        );
        assert_eq!(
            issuer.handshake("nobody", "s3cret"),
            Err(AuthError::BadCredentials),
            "an unknown user must not be distinguishable from a wrong password"
        );
        assert_eq!(issuer.issued_count(), 0, "a refusal issues nothing");
    }

    /// A token this server did not issue is not a token.
    #[test]
    fn an_invented_token_is_not_accepted() {
        let issuer = issuer(Arc::new(TestClock::new(0)));
        issuer.handshake("writer", "s3cret").unwrap();

        assert_eq!(
            issuer.validate("00").unwrap_err(),
            AuthError::Unauthenticated
        );
        assert_eq!(issuer.validate("").unwrap_err(), AuthError::Unauthenticated);
    }

    /// §9: a token expires, and the clock is injected so this is a test rather
    /// than a wait.
    #[test]
    fn a_token_expires_and_is_forgotten() {
        let clock = Arc::new(TestClock::new(0));
        let issuer = issuer(Arc::clone(&clock));
        let token = issuer.handshake("writer", "s3cret").unwrap();

        clock.set(DEFAULT_TOKEN_TTL - 1);
        assert!(issuer.validate(&token).is_ok(), "still inside the window");

        clock.set(DEFAULT_TOKEN_TTL);
        assert_eq!(
            issuer.validate(&token).unwrap_err(),
            AuthError::Unauthenticated
        );
        assert_eq!(
            issuer.issued_count(),
            0,
            "an expired session must not be kept, or the map grows forever"
        );
    }

    /// Two handshakes must never collide, or one client could act as another.
    #[test]
    fn every_handshake_issues_a_distinct_token() {
        let issuer = issuer(Arc::new(TestClock::new(0)));

        let first = issuer.handshake("writer", "s3cret").unwrap();
        let second = issuer.handshake("reader", "hunter2").unwrap();

        assert_ne!(first, second);
        assert_eq!(issuer.validate(&first).unwrap().name, "writer");
        assert_eq!(issuer.validate(&second).unwrap().name, "reader");
    }

    /// §9: the store is a seam. A backend that learns grants from the
    /// credential itself has to fit without changing anything above it.
    #[test]
    fn the_credential_store_is_pluggable() {
        #[derive(Debug)]
        struct AlwaysBob;
        impl CredentialStore for AlwaysBob {
            fn authenticate(&self, _user: &str, _password: &str) -> Option<Principal> {
                Some(Principal::new("bob", Grants::read_only(["events"])))
            }
        }

        let issuer = TokenIssuer::new(
            Arc::new(AlwaysBob),
            Arc::new(TestTokens::default()),
            Arc::new(TestClock::new(0)),
            DEFAULT_TOKEN_TTL,
        );
        let token = issuer.handshake("anyone", "anything").unwrap();

        let principal = issuer.validate(&token).unwrap();
        assert_eq!(principal.name, "bob");
        assert!(principal.may_read("events").is_ok());
        assert!(principal.may_write("events").is_err());
    }

    // ---- authorization (§9) ----

    /// §9: grants are per table and per direction. A reader may read the table
    /// it was granted and write nothing.
    #[test]
    fn grants_are_per_table_and_per_direction() {
        let reader = Principal::new("reader", Grants::read_only(["readings"]));

        assert!(reader.may_read("readings").is_ok());
        assert!(reader.may_read("events").is_err());
        assert!(reader.may_write("readings").is_err());
    }

    /// A grant nobody wrote down is not a grant — the default denies.
    #[test]
    fn an_ungranted_principal_may_do_nothing() {
        let nobody = Principal::new("nobody", Grants::default());

        assert!(nobody.may_read("readings").is_err());
        assert!(nobody.may_write("readings").is_err());
    }

    /// §6: tables appear as clients write to them, so `All` has to cover one
    /// that does not exist yet.
    #[test]
    fn a_blanket_grant_covers_a_table_that_does_not_exist_yet() {
        let admin = Principal::new("admin", Grants::all());
        assert!(admin.may_write("brand-new").is_ok());
    }

    /// A query naming several tables is granted only if every one of them is:
    /// a join reaching an ungranted table would leak it through the rows it
    /// selects.
    #[test]
    fn a_query_over_several_tables_needs_every_one_of_them() {
        let reader = Principal::new("reader", Grants::read_only(["readings"]));

        assert!(reader.may_read_all(["readings"]).is_ok());
        assert!(
            reader.may_read_all(["readings", "events"]).is_err(),
            "one ungranted table must refuse the whole query"
        );
    }

    /// The refusal has to name what was refused, or an operator cannot fix the
    /// grant.
    #[test]
    fn a_refusal_names_the_principal_the_action_and_the_table() {
        let error = Principal::new("reader", Grants::default())
            .may_write("readings")
            .expect_err("must be refused");

        let message = error.to_string();
        assert!(message.contains("reader"), "{message}");
        assert!(message.contains("write"), "{message}");
        assert!(message.contains("readings"), "{message}");
    }

    // ---- the header (§9) ----

    #[test]
    fn a_bearer_header_yields_its_token() {
        assert_eq!(bearer_token("Bearer abc123"), Some("abc123"));
        assert_eq!(
            bearer_token("bearer abc123"),
            Some("abc123"),
            "RFC 9110 makes the scheme case-insensitive"
        );
    }

    #[test]
    fn anything_that_is_not_a_bearer_header_yields_nothing() {
        for header in ["", "abc123", "Basic dXNlcjpwYXNz", "Bearer ", "Bearer  "] {
            assert_eq!(bearer_token(header), None, "{header:?} was accepted");
        }
    }

    /// A password comparison must not stop at the first differing byte.
    #[test]
    fn password_comparison_is_length_checked_and_total() {
        assert!(constant_time_eq(b"s3cret", b"s3cret"));
        assert!(!constant_time_eq(b"s3cret", b"s3cres"));
        assert!(!constant_time_eq(b"s3cret", b"s3cret "));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    /// The production source must produce something unpredictable, which shows
    /// as two draws differing — a counter or a clock would not.
    #[test]
    fn the_system_token_source_does_not_repeat() {
        let mut first = [0u8; TOKEN_BYTES];
        let mut second = [0u8; TOKEN_BYTES];
        SystemTokens.fill(&mut first);
        SystemTokens.fill(&mut second);

        assert_ne!(first, second);
        assert_ne!(first, [0u8; TOKEN_BYTES], "entropy must have reached it");
    }
}
