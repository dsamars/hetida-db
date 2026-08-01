// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Metric registry: series identity to `u64` surrogate.
//!
//! A series is identified by its metric name *and* its complete label set
//! (§10). The registry maps that identity onto a `u64` [`MetricId`], which is
//! what data files actually store — keeping the sort key fixed-width and the
//! fact table narrow, with labels held here alone.
//!
//! Registration is idempotent and durable before it is visible: a registration
//! reaches the log before any caller can see its id, so a crash never leaves an
//! id in use that no log records.
//!
//! The registry is off the ingestion path by design (§10): clients resolve ids
//! once and cache them, and `do_put` sees only `u64`. That is why an `RwLock`
//! is appropriate here where the concurrency rule would otherwise object.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, RwLock, RwLockWriteGuard};

/// Surrogate identifier of one series (§10).
pub type MetricId = u64;

/// Full identity of a series: metric name plus its complete label set.
///
/// Labels are held in a [`BTreeMap`], so equality and hashing ignore the order
/// a client happened to send them in — the same series always resolves to the
/// same id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SeriesKey {
    name: String,
    labels: BTreeMap<String, String>,
}

impl SeriesKey {
    /// Builds a series key from a metric name and its labels.
    pub fn new(
        name: impl Into<String>,
        labels: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Self {
            name: name.into(),
            labels: labels.into_iter().collect(),
        }
    }

    /// The metric name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The label set, in canonical order.
    pub fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }
}

/// One durable registration: the assignment of an id to a series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    /// The assigned surrogate.
    pub id: MetricId,
    /// The series it identifies.
    pub key: SeriesKey,
}

/// Sink making registrations durable.
///
/// §10 puts the registry through the WAL alongside ingested data; this is the
/// seam that connects the two once the WAL exists.
pub trait RegistrationLog: fmt::Debug + Send + Sync {
    /// Persists `registration`, returning only once it is durable.
    fn append(&self, registration: &Registration) -> Result<(), RegistryError>;
}

/// Why a registry operation failed.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// The registration could not be made durable, so no id was assigned.
    #[error("could not persist registration: {0}")]
    Persistence(Box<dyn std::error::Error + Send + Sync>),
}

/// Maps series identities onto surrogate ids.
#[derive(Debug)]
pub struct MetricRegistry {
    log: Arc<dyn RegistrationLog>,
    state: RwLock<RegistryState>,
}

#[derive(Debug, Default)]
struct RegistryState {
    by_series: HashMap<SeriesKey, MetricId>,
    by_id: HashMap<MetricId, SeriesKey>,
    next_id: MetricId,
}

impl MetricRegistry {
    /// Creates an empty registry persisting through `log`.
    pub fn new(log: Arc<dyn RegistrationLog>) -> Self {
        Self {
            log,
            state: RwLock::new(RegistryState::default()),
        }
    }

    /// Returns the id for `key`, assigning and persisting one if new.
    ///
    /// Idempotent: an already-registered series returns its existing id without
    /// touching the log. The whole check-assign-persist runs under the write
    /// lock, so two clients racing on the same series cannot split it across
    /// two ids.
    pub fn register(&self, key: SeriesKey) -> Result<MetricId, RegistryError> {
        let mut state = self.write();
        if let Some(id) = state.by_series.get(&key) {
            return Ok(*id);
        }

        let registration = Registration {
            id: state.next_id,
            key,
        };
        // Durable before visible: on failure nothing is inserted and `next_id`
        // stays put, so the id is not burnt either.
        self.log.append(&registration)?;
        state.insert(registration.clone());

        Ok(registration.id)
    }

    /// Returns the id of an already-registered series.
    pub fn resolve(&self, key: &SeriesKey) -> Option<MetricId> {
        self.read().by_series.get(key).copied()
    }

    /// Returns the series an id identifies.
    ///
    /// This is the direction a label predicate travels (§10): the registry is
    /// scanned to a set of ids, which then pushes down as a filter.
    pub fn series(&self, id: MetricId) -> Option<SeriesKey> {
        self.read().by_id.get(&id).cloned()
    }

    /// Number of registered series.
    pub fn len(&self) -> usize {
        self.read().by_series.len()
    }

    /// Whether no series is registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Restores registrations read back from the log.
    ///
    /// Does not write to the log — this *is* the log being read. Numbering
    /// continues past the highest id seen.
    pub fn replay(
        &self,
        registrations: impl IntoIterator<Item = Registration>,
    ) -> Result<(), RegistryError> {
        let mut state = self.write();
        for registration in registrations {
            state.insert(registration);
        }
        Ok(())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, RegistryState> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, RegistryState> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl RegistryState {
    /// Commits a registration and keeps `next_id` past it.
    fn insert(&mut self, registration: Registration) {
        self.by_series
            .insert(registration.key.clone(), registration.id);
        self.by_id.insert(registration.id, registration.key);
        self.next_id = self.next_id.max(registration.id.saturating_add(1));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    /// Recording [`RegistrationLog`] that can be told to fail.
    #[derive(Debug, Default)]
    struct RecordingLog {
        appended: Mutex<Vec<Registration>>,
        fail: Mutex<bool>,
    }

    impl RecordingLog {
        fn appended(&self) -> Vec<Registration> {
            self.appended.lock().unwrap().clone()
        }

        fn set_failing(&self, failing: bool) {
            *self.fail.lock().unwrap() = failing;
        }
    }

    impl RegistrationLog for RecordingLog {
        fn append(&self, registration: &Registration) -> Result<(), RegistryError> {
            if *self.fail.lock().unwrap() {
                return Err(RegistryError::Persistence("log is failing".into()));
            }
            self.appended.lock().unwrap().push(registration.clone());
            Ok(())
        }
    }

    fn registry() -> (Arc<RecordingLog>, MetricRegistry) {
        let log = Arc::new(RecordingLog::default());
        (Arc::clone(&log), MetricRegistry::new(log))
    }

    fn key(name: &str, labels: &[(&str, &str)]) -> SeriesKey {
        SeriesKey::new(
            name,
            labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        )
    }

    #[test]
    fn registering_assigns_an_id() {
        let (_log, registry) = registry();
        assert_eq!(registry.register(key("temp", &[])).unwrap(), 0);
        assert_eq!(registry.register(key("pressure", &[])).unwrap(), 1);
    }

    /// §10: registration is idempotent. Clients resolve once and cache, but a
    /// restarted client must get the same id back.
    #[test]
    fn registering_the_same_series_twice_returns_the_same_id() {
        let (log, registry) = registry();
        let first = registry.register(key("temp", &[("plant", "a")])).unwrap();
        let second = registry.register(key("temp", &[("plant", "a")])).unwrap();

        assert_eq!(first, second);
        assert_eq!(log.appended().len(), 1, "idempotent hit must not be logged");
    }

    /// §10: the id covers the whole series identity, name *and* labels. Were
    /// labels payload, two parallel series would collapse into one logical row
    /// and the second would look like a correction of the first.
    #[test]
    fn differing_labels_are_different_series() {
        let (_log, registry) = registry();
        let a = registry.register(key("temp", &[("plant", "a")])).unwrap();
        let b = registry.register(key("temp", &[("plant", "b")])).unwrap();
        let extra = registry
            .register(key("temp", &[("plant", "a"), ("line", "1")]))
            .unwrap();

        assert_ne!(a, b);
        assert_ne!(a, extra);
    }

    #[test]
    fn the_same_labels_in_a_different_order_are_the_same_series() {
        let (_log, registry) = registry();
        let forwards = registry
            .register(key("temp", &[("plant", "a"), ("line", "1")]))
            .unwrap();
        let backwards = registry
            .register(key("temp", &[("line", "1"), ("plant", "a")]))
            .unwrap();

        assert_eq!(forwards, backwards);
    }

    #[test]
    fn the_same_labels_under_a_different_metric_name_are_different_series() {
        let (_log, registry) = registry();
        let temp = registry.register(key("temp", &[("plant", "a")])).unwrap();
        let pressure = registry
            .register(key("pressure", &[("plant", "a")]))
            .unwrap();

        assert_ne!(temp, pressure);
    }

    #[test]
    fn an_unregistered_series_resolves_to_nothing() {
        let (_log, registry) = registry();
        assert_eq!(registry.resolve(&key("absent", &[])), None);
    }

    /// §10: labels live in the registry only, so the reverse direction has to
    /// work — that is how a label predicate becomes a `metric_id` filter.
    #[test]
    fn an_id_resolves_back_to_its_series() {
        let (_log, registry) = registry();
        let series = key("temp", &[("plant", "a")]);
        let id = registry.register(series.clone()).unwrap();

        assert_eq!(registry.series(id), Some(series));
    }

    /// §10: the registry is critical state. Nothing becomes visible before it
    /// is durable, or a crash would leave ids in use that no log records.
    #[test]
    fn a_failing_log_assigns_no_id() {
        let (log, registry) = registry();
        log.set_failing(true);

        let error = registry.register(key("temp", &[])).expect_err("must fail");
        assert!(matches!(error, RegistryError::Persistence(_)));
        assert_eq!(registry.resolve(&key("temp", &[])), None);

        // The id was not burnt either: the next successful registration takes it.
        log.set_failing(false);
        assert_eq!(registry.register(key("temp", &[])).unwrap(), 0);
    }

    #[test]
    fn replay_restores_ids_and_continues_numbering() {
        let (_log, registry) = registry();
        registry
            .replay(vec![
                Registration {
                    id: 4,
                    key: key("temp", &[]),
                },
                Registration {
                    id: 9,
                    key: key("pressure", &[]),
                },
            ])
            .unwrap();

        assert_eq!(registry.resolve(&key("temp", &[])), Some(4));
        assert_eq!(registry.resolve(&key("pressure", &[])), Some(9));
        assert_eq!(
            registry.register(key("flow", &[])).unwrap(),
            10,
            "numbering must continue past the highest replayed id"
        );
    }

    #[test]
    fn replay_does_not_write_back_to_the_log() {
        let (log, registry) = registry();
        registry
            .replay(vec![Registration {
                id: 0,
                key: key("temp", &[]),
            }])
            .unwrap();

        assert!(log.appended().is_empty(), "replay must not re-log");
    }

    /// Two clients registering the same series concurrently must not each get
    /// their own id — that would split one series in two.
    #[test]
    fn concurrent_registration_of_one_series_yields_one_id() {
        const THREADS: usize = 16;

        let (log, registry) = registry();
        let registry = Arc::new(registry);

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let registry = Arc::clone(&registry);
                thread::spawn(move || registry.register(key("temp", &[("plant", "a")])).unwrap())
            })
            .collect();

        let ids: Vec<MetricId> = handles
            .into_iter()
            .map(|handle| handle.join().expect("worker thread panicked"))
            .collect();

        assert!(
            ids.iter().all(|id| *id == ids[0]),
            "series was split across ids: {ids:?}"
        );
        assert_eq!(log.appended().len(), 1);
    }

    #[test]
    fn concurrent_registration_of_distinct_series_never_shares_an_id() {
        const THREADS: usize = 16;

        let (_log, registry) = registry();
        let registry = Arc::new(registry);

        let handles: Vec<_> = (0..THREADS)
            .map(|index| {
                let registry = Arc::clone(&registry);
                thread::spawn(move || {
                    registry
                        .register(key("temp", &[("line", &index.to_string())]))
                        .unwrap()
                })
            })
            .collect();

        let mut ids: Vec<MetricId> = handles
            .into_iter()
            .map(|handle| handle.join().expect("worker thread panicked"))
            .collect();
        ids.sort_unstable();
        ids.dedup();

        assert_eq!(ids.len(), THREADS);
    }
}
