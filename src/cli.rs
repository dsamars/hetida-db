// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The command line, which is deliberately almost empty.
//!
//! Everything an operator configures lives in the file `HETIDA_CONFIG` names —
//! one place, no precedence rules (§13, and `config`). What is left here is what
//! a configuration file cannot answer: which build is running, and what the
//! options are.
//!
//! An unrecognised argument is an **error**, never something to ignore. A server
//! started with a flag it silently drops is running with settings its operator
//! believes are in effect, which is the same failure the configuration file
//! rejects unknown keys to avoid.

/// The version this build reports, including the commit where one is known.
///
/// `build.rs` supplies the commit; a build from a source archive with no `.git`
/// has none, and then the package version stands alone rather than being
/// decorated with a guess.
pub fn version() -> String {
    match option_env!("HETIDA_BUILD_COMMIT") {
        Some(commit) if !commit.is_empty() => {
            format!("{} ({commit})", env!("CARGO_PKG_VERSION"))
        }
        _ => env!("CARGO_PKG_VERSION").to_owned(),
    }
}

/// What the command line asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Open the archive and serve it. The argument-free case.
    Serve,
    /// Report the version and stop.
    Version,
    /// Report the usage and stop.
    Help,
    /// An argument this build does not know.
    ///
    /// Carries it verbatim, so the message can name what was actually written
    /// rather than describing the category it fell into.
    Unknown(String),
}

/// Reads `arguments`, which must **not** include the program name.
pub fn parse<I, S>(arguments: I) -> Request
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut request = Request::Serve;
    for argument in arguments {
        match argument.as_ref() {
            // The first unknown argument wins over a later `--version`: a
            // command line with a mistake in it is not obeyed in part.
            "--version" | "-V" if request == Request::Serve => request = Request::Version,
            "--help" | "-h" if request == Request::Serve => request = Request::Help,
            "--version" | "-V" | "--help" | "-h" => {}
            other => return Request::Unknown(other.to_owned()),
        }
    }
    request
}

/// What `--help` prints.
///
/// Short on purpose: a longer one would be a second, competing account of the
/// configuration file, and the two would drift.
pub fn usage() -> String {
    format!(
        "hetida-db {}\n\
         \n\
         A bitemporal time-series archive, served over Arrow Flight.\n\
         \n\
         Usage: hetida-db [OPTIONS]\n\
         \n\
         Options:\n    \
             -V, --version  Print the version and exit\n    \
             -h, --help     Print this and exit\n\
         \n\
         Everything else is configured in the file HETIDA_CONFIG names — the\n\
         only environment variable this server reads. Without it the compiled\n\
         defaults apply. See CONFIGURATION.md.",
        version()
    )
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

    #[test]
    fn no_arguments_means_serve() {
        assert_eq!(parse(Vec::<String>::new()), Request::Serve);
    }

    #[test]
    fn both_spellings_of_each_flag_are_understood() {
        assert_eq!(parse(["--version"]), Request::Version);
        assert_eq!(parse(["-V"]), Request::Version);
        assert_eq!(parse(["--help"]), Request::Help);
        assert_eq!(parse(["-h"]), Request::Help);
    }

    /// An argument the build does not know is an error, not something to drop.
    /// A server started with a flag it ignored is running with settings its
    /// operator believes are in effect.
    #[test]
    fn an_unknown_argument_is_refused_by_name() {
        assert_eq!(
            parse(["--config", "/etc/hetida.toml"]),
            Request::Unknown("--config".to_owned())
        );
        assert_eq!(
            parse(["--verison"]),
            Request::Unknown("--verison".to_owned())
        );
    }

    /// A command line with a mistake in it is not obeyed in part: the mistake
    /// is reported even when something recognisable follows it.
    #[test]
    fn a_mistake_wins_over_a_later_valid_flag() {
        assert_eq!(
            parse(["--colour", "--version"]),
            Request::Unknown("--colour".to_owned())
        );
    }

    /// The version has to be the package's, so a report naming it can be
    /// matched to a release rather than to a guess.
    #[test]
    fn the_version_reports_the_package_version() {
        assert!(
            version().starts_with(env!("CARGO_PKG_VERSION")),
            "{}",
            version()
        );
    }

    /// The usage names both flags and points at the one environment variable,
    /// or it sends its reader looking in the wrong place.
    #[test]
    fn the_usage_names_the_flags_and_the_configuration() {
        let usage = usage();
        for expected in ["--version", "--help", "HETIDA_CONFIG", "CONFIGURATION.md"] {
            assert!(usage.contains(expected), "usage omits {expected}: {usage}");
        }
    }
}
