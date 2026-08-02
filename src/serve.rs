// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Transport and credential configuration for the server (§9).
//!
//! Two things an operator configures and the binary must refuse to guess at:
//! the TLS identity, and who may do what.
//!
//! # TLS is on unless it is turned off in writing
//!
//! A bearer token travels in a header. Without TLS it travels in the clear, and
//! anyone on the path can replay it — so a plaintext listener is a decision, not
//! a default, and [`TransportConfig::resolve`] makes it one that has to be
//! written down.
//!
//! # So is anonymous access
//!
//! The same for credentials. An archive serving an unauthenticated `do_put` to
//! anyone who can reach the port is a defensible choice on a private network and
//! a disaster anywhere else, and the difference is not something a default can
//! know.

use std::path::PathBuf;
use std::sync::Arc;

use crate::auth::{Grants, StaticCredentials, TableSet};

/// Why the server could not be configured.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// TLS was neither configured nor explicitly declined (§9).
    #[error(
        "no TLS identity configured. Set {CERT_VAR} and {KEY_VAR} to a PEM \
         certificate chain and private key, or set {PLAINTEXT_VAR}=1 to serve \
         in the clear — a bearer token in a header is readable by anyone on the \
         path (§9)"
    )]
    NoTls,
    /// Only one half of a TLS identity was given.
    #[error("{CERT_VAR} and {KEY_VAR} must be set together; only {given} was")]
    HalfTls {
        /// The one that was set.
        given: &'static str,
    },
    /// No credentials were configured and anonymous access was not declined.
    #[error(
        "no credentials configured. Set {USERS_VAR} to \
         `user:password:read=a,b:write=c` entries separated by `;`, or set \
         {ANONYMOUS_VAR}=1 to grant every caller everything (§9)"
    )]
    NoCredentials,
    /// A credential entry could not be read.
    #[error("{USERS_VAR} entry {index} is malformed: {reason}")]
    BadCredentials {
        /// Which entry, counting from one.
        index: usize,
        /// What was wrong with it.
        reason: String,
    },
}

/// PEM certificate chain for the listener.
pub const CERT_VAR: &str = "HETIDA_TLS_CERT";
/// PEM private key for the listener.
pub const KEY_VAR: &str = "HETIDA_TLS_KEY";
/// Set to `1` to serve without TLS, deliberately.
pub const PLAINTEXT_VAR: &str = "HETIDA_ALLOW_PLAINTEXT";
/// Credential entries, `;`-separated.
pub const USERS_VAR: &str = "HETIDA_USERS";
/// Set to `1` to serve without authentication, deliberately.
pub const ANONYMOUS_VAR: &str = "HETIDA_ALLOW_ANONYMOUS";

/// How the listener is protected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportConfig {
    /// TLS, from a PEM certificate chain and private key.
    Tls {
        /// Path to the certificate chain.
        certificate: PathBuf,
        /// Path to the private key.
        key: PathBuf,
    },
    /// In the clear, because an operator said so.
    Plaintext,
}

impl TransportConfig {
    /// Reads the transport from `lookup`, refusing to guess (§9).
    ///
    /// `lookup` is the environment, injected so this is testable without one.
    pub fn resolve(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        match (lookup(CERT_VAR), lookup(KEY_VAR)) {
            (Some(certificate), Some(key)) => Ok(Self::Tls {
                certificate: PathBuf::from(certificate),
                key: PathBuf::from(key),
            }),
            (Some(_), None) => Err(ConfigError::HalfTls { given: CERT_VAR }),
            (None, Some(_)) => Err(ConfigError::HalfTls { given: KEY_VAR }),
            (None, None) if enabled(&lookup, PLAINTEXT_VAR) => Ok(Self::Plaintext),
            (None, None) => Err(ConfigError::NoTls),
        }
    }
}

/// Who may do what, or an explicit decision to ask nobody.
#[derive(Debug)]
pub enum AuthConfig {
    /// Credentials, checked at every handshake.
    Users(Arc<StaticCredentials>),
    /// No authentication at all, because an operator said so.
    Anonymous,
}

impl AuthConfig {
    /// Reads credentials from `lookup`, refusing to guess (§9).
    ///
    /// The format is one entry per user, separated by `;`:
    /// `name:password:read=a,b:write=c`. A missing `read` or `write` clause
    /// grants nothing, and `*` grants every table — including ones that do not
    /// exist yet (§6).
    pub fn resolve(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let Some(entries) = lookup(USERS_VAR).filter(|text| !text.trim().is_empty()) else {
            return if enabled(&lookup, ANONYMOUS_VAR) {
                Ok(Self::Anonymous)
            } else {
                Err(ConfigError::NoCredentials)
            };
        };

        let mut credentials = StaticCredentials::new();
        for (position, entry) in entries
            .split(';')
            .filter(|e| !e.trim().is_empty())
            .enumerate()
        {
            let index = position + 1;
            let mut fields = entry.trim().split(':');
            let (Some(user), Some(password)) = (fields.next(), fields.next()) else {
                return Err(ConfigError::BadCredentials {
                    index,
                    reason: "expected at least `user:password`".to_owned(),
                });
            };
            if user.is_empty() {
                return Err(ConfigError::BadCredentials {
                    index,
                    reason: "the user name is empty".to_owned(),
                });
            }

            let mut grants = Grants::default();
            for clause in fields {
                let (kind, tables) =
                    clause
                        .split_once('=')
                        .ok_or_else(|| ConfigError::BadCredentials {
                            index,
                            reason: format!("`{clause}` is not `read=…` or `write=…`"),
                        })?;
                let set = table_set(tables);
                match kind {
                    "read" => grants.read = set,
                    "write" => grants.write = set,
                    other => {
                        return Err(ConfigError::BadCredentials {
                            index,
                            reason: format!("unknown grant `{other}`; expected read or write"),
                        });
                    }
                }
            }

            credentials = credentials.with_user(user, password, grants);
        }

        Ok(Self::Users(Arc::new(credentials)))
    }
}

/// A comma-separated table list, where `*` means every table.
fn table_set(tables: &str) -> TableSet {
    if tables.trim() == "*" {
        return TableSet::All;
    }
    TableSet::named(
        tables
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty()),
    )
}

/// Whether an opt-out variable is set to something affirmative.
fn enabled(lookup: &impl Fn(&str) -> Option<String>, name: &str) -> bool {
    lookup(name).is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
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
    use std::collections::HashMap;

    use crate::auth::CredentialStore;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    // ---- transport (§9) ----

    /// §9: a bearer token in a header is readable by anyone on the path, so
    /// plaintext is a decision and never a default.
    #[test]
    fn serving_without_tls_has_to_be_asked_for() {
        assert_eq!(
            TransportConfig::resolve(env(&[])),
            Err(ConfigError::NoTls),
            "an unconfigured server must refuse to start, not serve in the clear"
        );
        assert_eq!(
            TransportConfig::resolve(env(&[(PLAINTEXT_VAR, "1")])),
            Ok(TransportConfig::Plaintext)
        );
    }

    #[test]
    fn a_tls_identity_is_read_from_both_halves() {
        assert_eq!(
            TransportConfig::resolve(env(&[
                (CERT_VAR, "/tls/chain.pem"),
                (KEY_VAR, "/tls/key.pem")
            ])),
            Ok(TransportConfig::Tls {
                certificate: PathBuf::from("/tls/chain.pem"),
                key: PathBuf::from("/tls/key.pem"),
            })
        );
    }

    /// Half an identity is a mistake, not a request for plaintext.
    #[test]
    fn half_a_tls_identity_is_refused() {
        assert_eq!(
            TransportConfig::resolve(env(&[(CERT_VAR, "/tls/chain.pem")])),
            Err(ConfigError::HalfTls { given: CERT_VAR })
        );
        assert_eq!(
            TransportConfig::resolve(env(&[(KEY_VAR, "/tls/key.pem")])),
            Err(ConfigError::HalfTls { given: KEY_VAR })
        );
    }

    // ---- credentials (§9) ----

    /// §9: an archive serving `do_put` to anyone who can reach the port is a
    /// decision too.
    #[test]
    fn serving_without_credentials_has_to_be_asked_for() {
        assert!(matches!(
            AuthConfig::resolve(env(&[])),
            Err(ConfigError::NoCredentials)
        ));
        assert!(matches!(
            AuthConfig::resolve(env(&[(ANONYMOUS_VAR, "1")])),
            Ok(AuthConfig::Anonymous)
        ));
    }

    /// §9: per-table read and write grants, from configuration.
    #[test]
    fn credentials_carry_per_table_grants() {
        let AuthConfig::Users(store) = AuthConfig::resolve(env(&[(
            USERS_VAR,
            "writer:s3cret:read=*:write=*;reader:hunter2:read=readings,events",
        )]))
        .expect("configured") else {
            panic!("expected users");
        };

        let writer = store.authenticate("writer", "s3cret").expect("writer");
        assert!(
            writer.may_write("anything").is_ok(),
            "`*` covers new tables"
        );

        let reader = store.authenticate("reader", "hunter2").expect("reader");
        assert!(reader.may_read("events").is_ok());
        assert!(reader.may_read("secrets").is_err());
        assert!(
            reader.may_write("readings").is_err(),
            "a clause nobody wrote is not a grant"
        );
    }

    #[test]
    fn a_user_with_no_clauses_is_granted_nothing() {
        let AuthConfig::Users(store) =
            AuthConfig::resolve(env(&[(USERS_VAR, "ghost:pw")])).expect("configured")
        else {
            panic!("expected users");
        };

        let ghost = store.authenticate("ghost", "pw").expect("ghost");
        assert!(ghost.may_read("readings").is_err());
        assert!(ghost.may_write("readings").is_err());
    }

    #[test]
    fn a_malformed_entry_is_refused_rather_than_skipped() {
        for (entry, what) in [
            ("nopassword", "missing password"),
            (":pw:read=*", "empty user"),
            ("u:pw:readings", "clause without ="),
            ("u:pw:delete=*", "unknown grant"),
        ] {
            assert!(
                matches!(
                    AuthConfig::resolve(env(&[(USERS_VAR, entry)])),
                    Err(ConfigError::BadCredentials { .. })
                ),
                "{what} was accepted: {entry}"
            );
        }
    }

    /// A malformed entry must not silently leave a user unauthenticated while
    /// the rest of the file works — the whole configuration is refused.
    #[test]
    fn one_bad_entry_refuses_the_whole_configuration() {
        assert!(matches!(
            AuthConfig::resolve(env(&[(USERS_VAR, "good:pw:read=*;bad")])),
            Err(ConfigError::BadCredentials { index: 2, .. })
        ));
    }

    #[test]
    fn an_opt_out_needs_an_affirmative_value() {
        for value in ["0", "false", "no", "", "maybe"] {
            assert!(
                TransportConfig::resolve(env(&[(PLAINTEXT_VAR, value)])).is_err(),
                "{value:?} was read as consent"
            );
        }
        for value in ["1", "true", "yes", "TRUE", " 1 "] {
            assert!(
                TransportConfig::resolve(env(&[(PLAINTEXT_VAR, value)])).is_ok(),
                "{value:?} was not read as consent"
            );
        }
    }
}
