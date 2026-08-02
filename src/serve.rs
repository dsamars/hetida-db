// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Transport and credential decisions for the server (§9).
//!
//! Two things an operator configures and the binary must refuse to guess at:
//! the TLS identity, and who may do what. Both come from the configuration file
//! ([`crate::config`]); this module holds the decisions made from it.
//!
//! # TLS is on unless it is turned off in writing
//!
//! A bearer token travels in a header. Without TLS it travels in the clear, and
//! anyone on the path can replay it — so a plaintext listener is a decision, not
//! a default, and it has to be written down.
//!
//! # So is anonymous access
//!
//! The same for credentials. An archive serving an unauthenticated `do_put` to
//! anyone who can reach the port is a defensible choice on a private network and
//! a disaster anywhere else, and the difference is not something a default can
//! know.
//!
//! # Passwords belong in a file, not in the environment
//!
//! An environment variable is visible in `ps` and in the process's `/proc`
//! entry, to every user on the host. That is why credentials moved here, and why
//! `password_file` exists beside `password`: a secret mounted at mode 0400 is
//! never in the archive's own configuration at all, which is what a container
//! secret or a systemd credential gives you.
//!
//! A configuration file holding literal passwords that anyone can read is
//! reported ([`Warning::WorldReadableSecrets`]) rather than refused. The bit is
//! a hygiene signal, not an undecided policy — and failing a deploy over an
//! ownership arrangement this process cannot fully judge would be the wrong
//! trade, where refusing to *guess* at TLS is the right one.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::auth::{CredentialStore, Grants, Principal, StaticCredentials, TableSet};

/// Why the server could not be configured.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// TLS was neither configured nor explicitly declined (§9).
    #[error(
        "no TLS identity configured. Set `tls_certificate` and `tls_key` under \
         [server] to a PEM certificate chain and private key, or set \
         `allow_plaintext = true` to serve in the clear — a bearer token in a \
         header is readable by anyone on the path (§9)"
    )]
    NoTls,
    /// Only one half of a TLS identity was given.
    #[error("[server] `tls_certificate` and `tls_key` must be set together; only `{given}` was")]
    HalfTls {
        /// The one that was set.
        given: &'static str,
    },
    /// TLS was configured *and* declined, which cannot both be meant.
    #[error(
        "[server] sets a TLS identity and `allow_plaintext = true`; \
         remove one — this process will not choose which was meant"
    )]
    ContradictoryTls,
    /// No credentials were configured and anonymous access was not declined.
    #[error(
        "no credentials configured. Add [[auth.users]] entries, or set \
         `allow_anonymous = true` under [auth] to grant every caller \
         everything (§9)"
    )]
    NoCredentials,
    /// Users were configured *and* anonymous access allowed.
    #[error(
        "[auth] allows anonymous access and also lists users; remove one — \
         anonymous access grants everything, so the grants would mean nothing"
    )]
    ContradictoryAuth,
    /// A credential entry could not be used.
    #[error("[[auth.users]] `{user}`: {reason}")]
    BadCredentials {
        /// Which user the entry named.
        user: String,
        /// What was wrong with it.
        reason: String,
    },
}

/// Something worth telling an operator that is not worth refusing over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Warning {
    /// The configuration holds literal passwords and is readable beyond its owner.
    WorldReadableSecrets {
        /// The file in question.
        path: PathBuf,
        /// Its permission bits, as the filesystem reports them.
        mode: u32,
    },
}

impl std::fmt::Display for Warning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorldReadableSecrets { path, mode } => write!(
                f,
                "{} holds literal passwords and is mode {:o}; \
                 restrict it to its owner, or use `password_file` to keep the \
                 secret out of the configuration entirely",
                path.display(),
                mode & 0o777,
            ),
        }
    }
}

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
    /// The transport a `[server]` section describes, refusing to guess (§9).
    pub fn resolve(
        certificate: Option<&Path>,
        key: Option<&Path>,
        allow_plaintext: bool,
    ) -> Result<Self, ConfigError> {
        match (certificate, key) {
            (Some(_), _) | (_, Some(_)) if allow_plaintext => Err(ConfigError::ContradictoryTls),
            (Some(certificate), Some(key)) => Ok(Self::Tls {
                certificate: certificate.to_path_buf(),
                key: key.to_path_buf(),
            }),
            (Some(_), None) => Err(ConfigError::HalfTls {
                given: "tls_certificate",
            }),
            (None, Some(_)) => Err(ConfigError::HalfTls { given: "tls_key" }),
            (None, None) if allow_plaintext => Ok(Self::Plaintext),
            (None, None) => Err(ConfigError::NoTls),
        }
    }
}

/// Why a TLS identity could not be loaded (§9).
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A PEM file could not be read.
    #[error("could not read `{path}`: {source}")]
    Unreadable {
        /// The file that was named.
        path: PathBuf,
        /// What the operating system said.
        source: std::io::Error,
    },
    /// The identity was refused by the TLS stack.
    #[error("the TLS identity was refused: {0}")]
    Refused(String),
}

/// The server TLS configuration `transport` describes, or `None` for plaintext.
///
/// Reads both PEM files here, at startup, so a missing or unreadable identity
/// stops the start rather than failing the first connection that needs it.
pub fn tls_config(
    transport: &TransportConfig,
) -> Result<Option<tonic::transport::ServerTlsConfig>, TlsError> {
    let TransportConfig::Tls { certificate, key } = transport else {
        return Ok(None);
    };

    let chain = read_pem(certificate)?;
    let private = read_pem(key)?;
    Ok(Some(tonic::transport::ServerTlsConfig::new().identity(
        tonic::transport::Identity::from_pem(chain, private),
    )))
}

/// Reads a PEM file, reporting the path rather than only the errno.
fn read_pem(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Unreadable {
        path: path.to_path_buf(),
        source,
    })
}

/// One configured user, before their password has been read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSpec {
    /// The name they authenticate as.
    pub name: String,
    /// The password, literal.
    pub password: Option<String>,
    /// A file holding the password, so it need not be in the configuration.
    pub password_file: Option<PathBuf>,
    /// Tables they may read.
    pub read: TableSet,
    /// Tables they may write.
    pub write: TableSet,
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
    /// The credentials an `[auth]` section describes, refusing to guess (§9).
    ///
    /// A password named by `password_file` is read here, so a start fails on an
    /// unreadable secret rather than on the first handshake that needs it.
    pub fn resolve(users: &[UserSpec], allow_anonymous: bool) -> Result<Self, ConfigError> {
        if allow_anonymous {
            return if users.is_empty() {
                Ok(Self::Anonymous)
            } else {
                Err(ConfigError::ContradictoryAuth)
            };
        }
        if users.is_empty() {
            return Err(ConfigError::NoCredentials);
        }

        let mut credentials = StaticCredentials::new();
        for user in users {
            credentials = credentials.with_user(
                &user.name,
                user.password()?,
                Grants {
                    read: user.read.clone(),
                    write: user.write.clone(),
                },
            );
        }
        Ok(Self::Users(Arc::new(credentials)))
    }

    /// Whether any configured user has a literal password in the file.
    ///
    /// What the permission check is about — a configuration naming only
    /// `password_file` entries holds no secret to protect.
    pub fn holds_literal_secrets(users: &[UserSpec]) -> bool {
        users.iter().any(|user| user.password.is_some())
    }
}

impl UserSpec {
    /// The password, from the file if one is named and from the literal if not.
    fn password(&self) -> Result<String, ConfigError> {
        match (&self.password, &self.password_file) {
            (Some(_), Some(_)) => Err(self.bad(
                "`password` and `password_file` are both set; remove one — \
                 this process will not choose which was meant",
            )),
            (None, None) => Err(self.bad("no `password` or `password_file`")),
            (Some(literal), None) => Ok(literal.clone()),
            (None, Some(path)) => {
                let read = std::fs::read_to_string(path).map_err(|error| {
                    self.bad(&format!("cannot read `{}`: {error}", path.display()))
                })?;
                // One trailing newline, because that is what `echo` and every
                // secret-mounting tool leaves behind. Nothing else is trimmed:
                // a password may legitimately begin or end with a space.
                let secret = read.strip_suffix('\n').map_or(read.as_str(), |trimmed| {
                    trimmed.strip_suffix('\r').unwrap_or(trimmed)
                });
                if secret.is_empty() {
                    return Err(self.bad(&format!("`{}` is empty", path.display())));
                }
                Ok(secret.to_owned())
            }
        }
    }

    fn bad(&self, reason: &str) -> ConfigError {
        ConfigError::BadCredentials {
            user: self.name.clone(),
            reason: reason.to_owned(),
        }
    }
}

/// The environment variables this server used to be configured with (§9).
///
/// Every one of them moved into the configuration file. They are listed so a
/// start can *refuse* when one is still set, rather than ignore it: an operator
/// upgrading would otherwise deploy with `listen` silently back at its default,
/// or with credentials silently dropped.
pub const RETIRED_VARS: [(&str, &str); 6] = [
    ("HETIDA_DATA_DIR", "`data_dir`"),
    ("HETIDA_LISTEN", "[server] `listen`"),
    ("HETIDA_TLS_CERT", "[server] `tls_certificate`"),
    ("HETIDA_TLS_KEY", "[server] `tls_key`"),
    ("HETIDA_ALLOW_PLAINTEXT", "[server] `allow_plaintext`"),
    ("HETIDA_ALLOW_ANONYMOUS", "[auth] `allow_anonymous`"),
];

/// The retired variable `HETIDA_USERS`, which no longer has a direct equivalent.
pub const RETIRED_USERS_VAR: &str = "HETIDA_USERS";

/// Reports any retired variable that is still set, with where it moved.
///
/// Returns one line per variable found, or nothing when the environment is
/// clean.
pub fn retired_in_use(lookup: impl Fn(&str) -> Option<String>) -> Vec<String> {
    let mut found: Vec<String> = RETIRED_VARS
        .iter()
        .filter(|(name, _)| lookup(name).is_some())
        .map(|(name, moved_to)| format!("{name} moved into the configuration file as {moved_to}"))
        .collect();

    if lookup(RETIRED_USERS_VAR).is_some() {
        found.push(format!(
            "{RETIRED_USERS_VAR} moved into the configuration file as [[auth.users]] \
             entries — and a password in an environment variable is visible in `ps`, \
             which is why it moved"
        ));
    }
    found
}

/// Whether `path` is readable by anyone other than its owner.
///
/// `None` where the platform has no such notion, or the file cannot be
/// inspected — an unanswerable question is not a warning.
#[cfg(unix)]
pub fn readable_beyond_owner(path: &Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;

    let mode = std::fs::metadata(path).ok()?.permissions().mode();
    (mode & 0o077 != 0).then_some(mode)
}

/// Whether `path` is readable by anyone other than its owner.
#[cfg(not(unix))]
pub fn readable_beyond_owner(_path: &Path) -> Option<u32> {
    None
}

/// A store that authenticates nobody, for a server configured anonymous.
///
/// Exists so the anonymous path has a principal type at all rather than a
/// special case threaded through the service.
#[derive(Debug, Default)]
pub struct NoCredentials;

impl CredentialStore for NoCredentials {
    fn authenticate(&self, _user: &str, _password: &str) -> Option<Principal> {
        None
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

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn user(name: &str, password: Option<&str>, file: Option<&Path>) -> UserSpec {
        UserSpec {
            name: name.to_owned(),
            password: password.map(ToOwned::to_owned),
            password_file: file.map(Path::to_path_buf),
            read: TableSet::named(["readings"]),
            write: TableSet::default(),
        }
    }

    // ---- transport (§9) ----

    /// §9: a bearer token in a header is readable by anyone on the path, so
    /// plaintext is a decision and never a default.
    #[test]
    fn serving_without_tls_has_to_be_asked_for() {
        assert_eq!(
            TransportConfig::resolve(None, None, false),
            Err(ConfigError::NoTls),
            "an unconfigured server must refuse to start, not serve in the clear"
        );
        assert_eq!(
            TransportConfig::resolve(None, None, true),
            Ok(TransportConfig::Plaintext)
        );
    }

    #[test]
    fn a_tls_identity_is_read_from_both_halves() {
        assert_eq!(
            TransportConfig::resolve(
                Some(Path::new("/tls/chain.pem")),
                Some(Path::new("/tls/key.pem")),
                false
            ),
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
            TransportConfig::resolve(Some(Path::new("/tls/chain.pem")), None, false),
            Err(ConfigError::HalfTls {
                given: "tls_certificate"
            })
        );
        assert_eq!(
            TransportConfig::resolve(None, Some(Path::new("/tls/key.pem")), false),
            Err(ConfigError::HalfTls { given: "tls_key" })
        );
    }

    /// Configuring TLS *and* declining it cannot both be meant, and picking one
    /// would be exactly the guess this whole area refuses to make.
    #[test]
    fn a_tls_identity_beside_allow_plaintext_is_refused() {
        assert_eq!(
            TransportConfig::resolve(
                Some(Path::new("/tls/chain.pem")),
                Some(Path::new("/tls/key.pem")),
                true
            ),
            Err(ConfigError::ContradictoryTls)
        );
    }

    // ---- credentials (§9) ----

    #[test]
    fn serving_without_credentials_has_to_be_asked_for() {
        assert!(matches!(
            AuthConfig::resolve(&[], false),
            Err(ConfigError::NoCredentials)
        ));
        assert!(matches!(
            AuthConfig::resolve(&[], true),
            Ok(AuthConfig::Anonymous)
        ));
    }

    /// Anonymous access grants everything, so listed grants would mean nothing.
    #[test]
    fn users_beside_allow_anonymous_are_refused() {
        assert!(matches!(
            AuthConfig::resolve(&[user("a", Some("pw"), None)], true),
            Err(ConfigError::ContradictoryAuth)
        ));
    }

    #[test]
    fn a_literal_password_authenticates() {
        let AuthConfig::Users(store) =
            AuthConfig::resolve(&[user("reader", Some("hunter2"), None)], false).expect("resolved")
        else {
            panic!("expected users");
        };

        let principal = store.authenticate("reader", "hunter2").expect("accepted");
        assert!(principal.may_read("readings").is_ok());
        assert!(principal.may_write("readings").is_err());
        assert!(store.authenticate("reader", "wrong").is_none());
    }

    /// §9: the point of the move. A secret mounted at mode 0400 is never in the
    /// archive's own configuration at all.
    #[test]
    fn a_password_file_keeps_the_secret_out_of_the_configuration() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("ingest.secret");
        // With the trailing newline `echo` and every secret mount leaves.
        std::fs::write(&path, "s3cret\n").expect("write");

        let AuthConfig::Users(store) =
            AuthConfig::resolve(&[user("ingest", None, Some(&path))], false).expect("resolved")
        else {
            panic!("expected users");
        };

        assert!(
            store.authenticate("ingest", "s3cret").is_some(),
            "the trailing newline must not be part of the password"
        );
    }

    #[test]
    fn a_password_file_keeps_leading_and_trailing_spaces() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("spaced.secret");
        std::fs::write(&path, " pass word \n").expect("write");

        let AuthConfig::Users(store) =
            AuthConfig::resolve(&[user("ingest", None, Some(&path))], false).expect("resolved")
        else {
            panic!("expected users");
        };

        assert!(
            store.authenticate("ingest", " pass word ").is_some(),
            "only the newline is trimmed — a password may end in a space"
        );
    }

    /// A start must fail on an unreadable secret, not on the first handshake
    /// that needs it hours later.
    #[test]
    fn an_unreadable_password_file_stops_the_start() {
        let missing = Path::new("/nonexistent/ingest.secret");
        assert!(matches!(
            AuthConfig::resolve(&[user("ingest", None, Some(missing))], false),
            Err(ConfigError::BadCredentials { .. })
        ));
    }

    #[test]
    fn an_empty_password_file_is_refused() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("empty.secret");
        std::fs::write(&path, "\n").expect("write");

        assert!(matches!(
            AuthConfig::resolve(&[user("ingest", None, Some(&path))], false),
            Err(ConfigError::BadCredentials { .. })
        ));
    }

    #[test]
    fn a_user_with_neither_password_nor_file_is_refused() {
        assert!(matches!(
            AuthConfig::resolve(&[user("ghost", None, None)], false),
            Err(ConfigError::BadCredentials { .. })
        ));
    }

    #[test]
    fn a_user_with_both_a_password_and_a_file_is_refused() {
        let path = Path::new("/tmp/whatever.secret");
        assert!(matches!(
            AuthConfig::resolve(&[user("ingest", Some("pw"), Some(path))], false),
            Err(ConfigError::BadCredentials { .. })
        ));
    }

    /// The refusal has to name the user, or an operator is left bisecting a
    /// list of them.
    #[test]
    fn a_credential_refusal_names_the_user() {
        let error = AuthConfig::resolve(&[user("ingest", None, None)], false).expect_err("refused");
        assert!(error.to_string().contains("ingest"), "{error}");
    }

    #[test]
    fn only_literal_passwords_are_a_secret_in_the_file() {
        assert!(AuthConfig::holds_literal_secrets(&[user(
            "a",
            Some("pw"),
            None
        )]));
        assert!(!AuthConfig::holds_literal_secrets(&[user(
            "a",
            None,
            Some(Path::new("/run/secrets/a"))
        )]));
        assert!(!AuthConfig::holds_literal_secrets(&[]));
    }

    // ---- the retired environment (§9) ----

    /// An operator upgrading would otherwise deploy with `listen` silently back
    /// at its default, or with credentials silently dropped. Ignoring a variable
    /// that used to work is the failure this exists to prevent.
    #[test]
    fn a_retired_variable_is_reported_rather_than_ignored() {
        let found = retired_in_use(env(&[("HETIDA_LISTEN", "0.0.0.0:50051")]));

        assert_eq!(found.len(), 1);
        assert!(found[0].contains("HETIDA_LISTEN"), "{found:?}");
        assert!(found[0].contains("listen"), "{found:?}");
    }

    #[test]
    fn every_retired_variable_is_reported_with_where_it_went() {
        let all: Vec<(&str, &str)> = RETIRED_VARS
            .iter()
            .map(|(name, _)| (*name, "x"))
            .chain([(RETIRED_USERS_VAR, "x")])
            .collect();

        let found = retired_in_use(env(&all));
        assert_eq!(found.len(), RETIRED_VARS.len() + 1);
    }

    #[test]
    fn a_clean_environment_reports_nothing() {
        assert!(retired_in_use(env(&[("HETIDA_CONFIG", "/etc/hetida.toml")])).is_empty());
    }

    /// The credentials variable is called out specially: it is the one whose
    /// replacement is not a like-for-like key, and the one whose old form was a
    /// security problem.
    #[test]
    fn the_credentials_variable_explains_why_it_moved() {
        let found = retired_in_use(env(&[("HETIDA_USERS", "a:b:read=*")]));
        assert_eq!(found.len(), 1);
        assert!(found[0].contains("ps"), "{found:?}");
    }

    // ---- the permission warning ----

    #[cfg(unix)]
    #[test]
    fn a_configuration_readable_by_others_is_reported() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("hetida.toml");
        std::fs::write(&path, "").expect("write");

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        assert!(
            readable_beyond_owner(&path).is_some(),
            "0644 must be flagged"
        );

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        assert_eq!(
            readable_beyond_owner(&path),
            None,
            "0600 is what the warning asks for"
        );
    }

    /// An unanswerable question is not a warning.
    #[test]
    fn a_missing_file_is_not_flagged() {
        assert_eq!(
            readable_beyond_owner(Path::new("/nonexistent/x.toml")),
            None
        );
    }

    #[test]
    fn the_warning_says_what_to_do_about_it() {
        let warning = Warning::WorldReadableSecrets {
            path: PathBuf::from("/etc/hetida.toml"),
            mode: 0o644,
        };
        let message = warning.to_string();

        assert!(message.contains("644"), "{message}");
        assert!(message.contains("password_file"), "{message}");
    }
}
