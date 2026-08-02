// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Write-ahead log.
//!
//! An append-only log of length-prefixed, CRC-covered records (§4). The log is
//! payload-agnostic: it stores opaque bytes under a [`RecordKind`], so the
//! encoding of a batch or a registration stays with the layer that owns it.
//!
//! # Durability
//!
//! [`Wal::append`] returns only once the record is on the device, so *acked*
//! means *durable*. Concurrent appends share one fsync — a background writer
//! drains everything already queued and syncs once. Paying an fsync per record
//! would cost about a millisecond each and cap throughput far below the
//! 1024-client target.
//!
//! [`WalConfig::max_commit_delay`] widens that window by waiting for stragglers
//! before syncing. Zero means group only what is already queued, which adds no
//! latency and keeps the writer deterministic under test.
//!
//! # Recovery
//!
//! [`replay`] reads segments in order and stops at the first record it cannot
//! trust — a short header, a length running past the end of the segment, or a
//! CRC mismatch. All three are the shape a crash mid-write leaves, and none of
//! them may prevent startup (§4). Damage is only ever at the tail, because
//! opening the log always starts a *new* segment rather than appending to one
//! whose end may already be torn.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::seq::Seq;
use crate::storage::{AppendFile, Storage, StorageError};

/// Bytes of framing ahead of each record's payload: length, CRC, kind, `seq`.
pub const HEADER_BYTES: usize = 4 + 4 + 1 + 8;

/// What a record carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    /// Ingested rows.
    Data = 1,
    /// A metric registration (§10).
    Registration = 2,
    /// Everything at or below this `seq` is in Parquet (§4).
    Checkpoint = 3,
    /// A table's schema, as accepted or evolved (§6).
    Schema = 4,
}

impl RecordKind {
    /// Reads a kind back from its on-disk byte.
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Data),
            2 => Some(Self::Registration),
            3 => Some(Self::Checkpoint),
            4 => Some(Self::Schema),
            _ => None,
        }
    }
}

/// One logged record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// What the payload is.
    pub kind: RecordKind,
    /// Position in the total order (§1); drives checkpoint reclamation.
    pub seq: Seq,
    /// Opaque bytes owned by the layer above.
    pub payload: Vec<u8>,
}

/// How the log is sized and how long it waits to group commits.
#[derive(Debug, Clone, Copy)]
pub struct WalConfig {
    /// Rotate to a new segment once the active one passes this size (§4).
    pub segment_bytes: u64,
    /// How long the writer waits for stragglers before syncing.
    ///
    /// Zero groups only what is already queued: no added latency, and
    /// deterministic under test.
    pub max_commit_delay: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 64 * 1024 * 1024,
            max_commit_delay: Duration::from_millis(10),
        }
    }
}

/// Why a log operation failed.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// The underlying store failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The background writer is gone, so nothing can be made durable.
    #[error("the write-ahead log writer has stopped")]
    WriterStopped,
    /// The group this record was committed with failed to reach the device.
    ///
    /// Carries the rendered cause rather than the original error: one failed
    /// write fails every caller in its group, and errors are not cloneable.
    #[error("write-ahead log write failed: {0}")]
    GroupFailed(String),
}

/// What a replay recovered.
#[derive(Debug, Default)]
pub struct Replayed {
    /// Records in the order they were written.
    pub records: Vec<WalRecord>,
    /// Segment whose tail was discarded, if any.
    pub truncated: Option<PathBuf>,
}

/// Encodes a record into its on-disk framing.
fn encode(record: &WalRecord) -> Vec<u8> {
    let mut body = Vec::with_capacity(9 + record.payload.len());
    body.push(record.kind as u8);
    body.extend_from_slice(&record.seq.to_le_bytes());
    body.extend_from_slice(&record.payload);

    let mut framed = Vec::with_capacity(8 + body.len());
    framed.extend_from_slice(&(record.payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    framed.extend_from_slice(&body);
    framed
}

/// Reads the records of one segment, stopping at the first untrustworthy one.
///
/// Returns the records and whether the segment's tail was discarded.
fn decode_segment(bytes: &[u8]) -> (Vec<WalRecord>, bool) {
    let mut records = Vec::new();
    let mut offset = 0usize;

    loop {
        let Some(header) = bytes.get(offset..offset + HEADER_BYTES) else {
            // Not even a full header left: a crash landed mid-framing.
            return (records, offset < bytes.len());
        };
        let (length_bytes, rest) = header.split_at(4);
        let (crc_bytes, kind_and_seq) = rest.split_at(4);

        let length = u32::from_le_bytes(length_bytes.try_into().unwrap_or_default()) as usize;
        let expected_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap_or_default());

        let body_start = offset + 8;
        let body_end = body_start + 9 + length;
        let Some(body) = bytes.get(body_start..body_end) else {
            // The length promises more than the segment holds.
            return (records, true);
        };
        if crc32fast::hash(body) != expected_crc {
            return (records, true);
        }

        let Some(kind) = kind_and_seq
            .first()
            .copied()
            .and_then(RecordKind::from_byte)
        else {
            return (records, true);
        };
        let seq = kind_and_seq
            .get(1..9)
            .and_then(|raw| raw.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or_default();

        records.push(WalRecord {
            kind,
            seq,
            payload: body.get(9..).unwrap_or_default().to_vec(),
        });
        offset = body_end;
    }
}

/// Names a segment so lexicographic order matches write order.
fn segment_name(number: u64) -> String {
    format!("{number:012}.wal")
}

/// Reads every segment under `dir` in order, discarding an untrustworthy tail.
///
/// Never fails on damage: a torn tail is the expected state after a crash, and
/// refusing to start would turn a recoverable crash into an outage (§4).
pub fn replay(storage: &dyn Storage, dir: &Path) -> Result<Replayed, WalError> {
    let mut replayed = Replayed::default();

    for segment in storage.list(dir)? {
        let bytes = storage.read(&segment)?;
        let (records, truncated) = decode_segment(&bytes);
        replayed.records.extend(records);
        if truncated {
            replayed.truncated = Some(segment);
            // Damage is only ever at the tail, so nothing beyond it is trusted.
            break;
        }
    }

    Ok(replayed)
}

/// Writes records to the log. Single-threaded; [`Wal`] owns one of these.
#[derive(Debug)]
pub struct WalWriter {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    config: WalConfig,
    sealed: Vec<(PathBuf, Seq)>,
    active: PathBuf,
    active_file: Box<dyn AppendFile>,
    active_bytes: u64,
    active_max_seq: Seq,
    next_segment: u64,
}

impl WalWriter {
    /// Opens the log under `dir`, always starting a fresh segment.
    ///
    /// Existing segments are scanned only to learn their highest `seq`, which
    /// reclamation needs. Appending to the last one instead would risk writing
    /// past a torn tail and burying the damage mid-segment, where replay could
    /// no longer treat it as the end.
    pub fn open(
        storage: Arc<dyn Storage>,
        dir: PathBuf,
        config: WalConfig,
    ) -> Result<Self, WalError> {
        let existing = storage.list(&dir)?;
        let mut sealed = Vec::with_capacity(existing.len());
        for segment in &existing {
            let bytes = storage.read(segment)?;
            let (records, _) = decode_segment(&bytes);
            let max_seq = records.iter().map(|record| record.seq).max().unwrap_or(0);
            sealed.push((segment.clone(), max_seq));
        }

        let next_segment = existing.len() as u64;
        let active = dir.join(segment_name(next_segment));
        let active_file = storage.open_append(&active)?;

        Ok(Self {
            storage,
            dir,
            config,
            sealed,
            active,
            active_file,
            active_bytes: 0,
            active_max_seq: 0,
            next_segment: next_segment + 1,
        })
    }

    /// Appends every record and syncs once (§4).
    pub fn write_all(&mut self, records: &[WalRecord]) -> Result<(), WalError> {
        if records.is_empty() {
            return Ok(());
        }

        let mut framed = Vec::new();
        let mut highest = self.active_max_seq;
        for record in records {
            framed.extend_from_slice(&encode(record));
            highest = highest.max(record.seq);
        }

        self.active_file.append(&framed)?;
        self.active_file.sync()?;
        self.active_bytes += framed.len() as u64;
        self.active_max_seq = highest;

        if self.active_bytes >= self.config.segment_bytes {
            self.rotate()?;
        }
        Ok(())
    }

    /// Seals the active segment and opens the next one.
    fn rotate(&mut self) -> Result<(), WalError> {
        let sealed = std::mem::replace(
            &mut self.active,
            self.dir.join(segment_name(self.next_segment)),
        );
        self.sealed.push((sealed, self.active_max_seq));

        self.active_file = self.storage.open_append(&self.active)?;
        self.active_bytes = 0;
        self.active_max_seq = 0;
        self.next_segment += 1;
        Ok(())
    }

    /// Deletes sealed segments whose records are all at or below `watermark`.
    ///
    /// The active segment is never dropped, however low its contents: it is
    /// still being appended to.
    pub fn reclaim(&mut self, watermark: Seq) -> Result<usize, WalError> {
        let mut kept = Vec::with_capacity(self.sealed.len());
        let mut dropped = 0;

        for (segment, max_seq) in std::mem::take(&mut self.sealed) {
            if max_seq <= watermark {
                self.storage.remove(&segment)?;
                dropped += 1;
            } else {
                kept.push((segment, max_seq));
            }
        }

        self.sealed = kept;
        Ok(dropped)
    }
}

/// One queued append and the caller waiting on its durability.
type Commit = (WalRecord, Sender<Result<(), WalError>>);

/// The write-ahead log: concurrent appends, one shared fsync.
#[derive(Debug)]
pub struct Wal {
    commits: Option<Sender<Commit>>,
    writer: Option<JoinHandle<()>>,
}

impl Wal {
    /// Opens the log under `dir` and starts its background writer.
    pub fn open(
        storage: Arc<dyn Storage>,
        dir: PathBuf,
        config: WalConfig,
    ) -> Result<Self, WalError> {
        let writer = WalWriter::open(storage, dir, config)?;
        let (commits, queue) = channel::<Commit>();

        let handle = std::thread::Builder::new()
            .name("hetida-wal".to_owned())
            .spawn(move || run_writer(writer, queue, config.max_commit_delay))
            .map_err(|_| WalError::WriterStopped)?;

        Ok(Self {
            commits: Some(commits),
            writer: Some(handle),
        })
    }

    /// Appends a record, returning once it is durable.
    pub fn append(&self, kind: RecordKind, seq: Seq, payload: Vec<u8>) -> Result<(), WalError> {
        let (done, wait) = channel();
        let record = WalRecord { kind, seq, payload };

        self.commits
            .as_ref()
            .ok_or(WalError::WriterStopped)?
            .send((record, done))
            .map_err(|_| WalError::WriterStopped)?;

        wait.recv().map_err(|_| WalError::WriterStopped)?
    }

    /// Records that everything at or below `seq` is in Parquet (§4).
    pub fn checkpoint(&self, seq: Seq) -> Result<(), WalError> {
        self.append(RecordKind::Checkpoint, seq, Vec::new())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Closing the channel ends the writer loop; joining it guarantees the
        // last group reached the device before the log is considered gone.
        drop(self.commits.take());
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

/// Drains queued commits, writing each group with a single fsync.
fn run_writer(mut writer: WalWriter, queue: Receiver<Commit>, max_delay: Duration) {
    while let Ok(first) = queue.recv() {
        let mut group = vec![first];
        gather(&queue, &mut group, max_delay);

        let records: Vec<WalRecord> = group.iter().map(|(record, _)| record.clone()).collect();
        let outcome = writer.write_all(&records);

        // One fsync covers the whole group, so its outcome is theirs alike.
        let reported = outcome.as_ref().err().map(WalError::to_string);
        for (_, done) in group {
            // A caller that stopped waiting is not the writer's problem; the
            // record is durable either way.
            let _ = done.send(match &reported {
                None => Ok(()),
                Some(cause) => Err(WalError::GroupFailed(cause.clone())),
            });
        }
    }
}

/// Collects further commits into the group about to be synced.
fn gather(queue: &Receiver<Commit>, group: &mut Vec<Commit>, max_delay: Duration) {
    if max_delay.is_zero() {
        // Take whatever is already queued and sync immediately.
        while let Ok(next) = queue.try_recv() {
            group.push(next);
        }
        return;
    }

    let deadline = Instant::now() + max_delay;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return;
        };
        match queue.recv_timeout(remaining) {
            Ok(next) => group.push(next),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => return,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::storage::{Fault, InMemoryStorage};

    fn dir() -> PathBuf {
        PathBuf::from("wal")
    }

    fn config() -> WalConfig {
        WalConfig {
            segment_bytes: 1024,
            // Zero keeps the writer deterministic: it groups whatever is
            // already queued and never waits on a timer.
            max_commit_delay: Duration::ZERO,
        }
    }

    fn writer(storage: &InMemoryStorage) -> WalWriter {
        WalWriter::open(Arc::new(storage.clone()), dir(), config()).unwrap()
    }

    fn record(seq: Seq, payload: &[u8]) -> WalRecord {
        WalRecord {
            kind: RecordKind::Data,
            seq,
            payload: payload.to_vec(),
        }
    }

    fn replay_of(storage: &InMemoryStorage) -> Vec<WalRecord> {
        replay(storage, &dir()).unwrap().records
    }

    #[test]
    fn a_written_record_replays_identically() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        writer.write_all(&[record(7, b"payload")]).unwrap();

        assert_eq!(replay_of(&storage), vec![record(7, b"payload")]);
    }

    #[test]
    fn records_replay_in_the_order_they_were_written() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        for seq in 0..5 {
            writer.write_all(&[record(seq, &[seq as u8])]).unwrap();
        }

        let seqs: Vec<Seq> = replay_of(&storage).iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn every_record_kind_round_trips() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        let written = vec![
            WalRecord {
                kind: RecordKind::Data,
                seq: 1,
                payload: b"batch".to_vec(),
            },
            WalRecord {
                kind: RecordKind::Registration,
                seq: 2,
                payload: b"series".to_vec(),
            },
            WalRecord {
                kind: RecordKind::Checkpoint,
                seq: 3,
                payload: Vec::new(),
            },
        ];
        writer.write_all(&written).unwrap();

        assert_eq!(replay_of(&storage), written);
    }

    #[test]
    fn an_empty_log_replays_empty() {
        let storage = InMemoryStorage::new();
        assert!(replay_of(&storage).is_empty());
    }

    /// §4: concurrent writes share one fsync. Paying an fsync per record would
    /// cap throughput far below the 1024-client target.
    #[test]
    fn a_batch_of_records_costs_exactly_one_fsync() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        let before = storage.syncs();

        writer
            .write_all(&[record(0, b"a"), record(1, b"b"), record(2, b"c")])
            .unwrap();

        assert_eq!(storage.syncs() - before, 1);
        assert_eq!(replay_of(&storage).len(), 3);
    }

    /// §4: a torn tail from a crash mid-write truncates the log there. It must
    /// never prevent startup.
    #[test]
    fn a_torn_tail_record_is_discarded_and_the_rest_survives() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        writer.write_all(&[record(0, b"intact")]).unwrap();

        storage.inject(Fault::TornWrite { keep: 6 });
        writer
            .write_all(&[record(1, b"lost to the crash")])
            .unwrap();

        let replayed = replay_of(&storage);
        assert_eq!(replayed, vec![record(0, b"intact")]);
    }

    /// A record whose bytes were corrupted rather than truncated must be caught
    /// too — that is what the CRC is for.
    #[test]
    fn a_corrupted_record_truncates_the_log_at_that_point() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        writer
            .write_all(&[record(0, b"first"), record(1, b"second")])
            .unwrap();

        // Flip a byte inside the second record's payload.
        let segment = storage.list(&dir()).unwrap().pop().unwrap();
        let mut bytes = storage.read(&segment).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        storage.remove(&segment).unwrap();
        let mut handle = storage.open_append(&segment).unwrap();
        handle.append(&bytes).unwrap();

        assert_eq!(replay_of(&storage), vec![record(0, b"first")]);
    }

    #[test]
    fn a_header_cut_in_half_is_discarded() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        writer.write_all(&[record(0, b"intact")]).unwrap();

        // Four bytes cannot even hold a length prefix and a checksum.
        storage.inject(Fault::TornWrite { keep: 4 });
        writer.write_all(&[record(1, b"truncated header")]).unwrap();

        assert_eq!(replay_of(&storage), vec![record(0, b"intact")]);
    }

    #[test]
    fn a_length_pointing_past_the_end_is_discarded() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        writer.write_all(&[record(0, b"intact")]).unwrap();

        // Header plus a couple of payload bytes: the length says more follows.
        storage.inject(Fault::TornWrite {
            keep: HEADER_BYTES + 2,
        });
        writer
            .write_all(&[record(1, b"a much longer payload")])
            .unwrap();

        assert_eq!(replay_of(&storage), vec![record(0, b"intact")]);
    }

    /// Opening never appends to an existing segment: its tail may be torn, and
    /// writing past the damage would bury it beyond recovery.
    #[test]
    fn opening_starts_a_fresh_segment() {
        let storage = InMemoryStorage::new();
        writer(&storage)
            .write_all(&[record(0, b"first run")])
            .unwrap();
        writer(&storage)
            .write_all(&[record(1, b"second run")])
            .unwrap();

        assert_eq!(storage.list(&dir()).unwrap().len(), 2);
        assert_eq!(replay_of(&storage).len(), 2);
    }

    /// §4: one segment per N bytes.
    #[test]
    fn segments_rotate_once_they_pass_the_size_limit() {
        let storage = InMemoryStorage::new();
        let mut writer = WalWriter::open(
            Arc::new(storage.clone()),
            dir(),
            WalConfig {
                segment_bytes: 64,
                max_commit_delay: Duration::ZERO,
            },
        )
        .unwrap();

        for seq in 0..10 {
            writer.write_all(&[record(seq, &[b'x'; 32])]).unwrap();
        }

        assert!(
            storage.list(&dir()).unwrap().len() > 1,
            "the log should have rotated"
        );
        assert_eq!(replay_of(&storage).len(), 10);
    }

    #[test]
    fn segments_replay_across_a_rotation_in_order() {
        let storage = InMemoryStorage::new();
        let mut writer = WalWriter::open(
            Arc::new(storage.clone()),
            dir(),
            WalConfig {
                segment_bytes: 64,
                max_commit_delay: Duration::ZERO,
            },
        )
        .unwrap();
        for seq in 0..20 {
            writer.write_all(&[record(seq, &[b'y'; 16])]).unwrap();
        }

        let seqs: Vec<Seq> = replay_of(&storage).iter().map(|r| r.seq).collect();
        assert_eq!(seqs, (0..20).collect::<Vec<_>>());
    }

    /// §4: a segment may be dropped once everything it covers is in Parquet.
    #[test]
    fn reclaiming_drops_only_segments_fully_below_the_watermark() {
        let storage = InMemoryStorage::new();
        let mut writer = WalWriter::open(
            Arc::new(storage.clone()),
            dir(),
            WalConfig {
                segment_bytes: 48,
                max_commit_delay: Duration::ZERO,
            },
        )
        .unwrap();
        for seq in 0..10 {
            writer.write_all(&[record(seq, &[b'z'; 24])]).unwrap();
        }
        let before = storage.list(&dir()).unwrap().len();

        let dropped = writer.reclaim(3).unwrap();

        assert!(dropped > 0, "segments below the watermark must be dropped");
        assert_eq!(storage.list(&dir()).unwrap().len(), before - dropped);
        let survivors: Vec<Seq> = replay_of(&storage).iter().map(|r| r.seq).collect();
        assert!(
            survivors.contains(&9),
            "everything above the watermark must survive"
        );
        assert!(
            !survivors.contains(&0),
            "the first segment was entirely below the watermark"
        );
    }

    #[test]
    fn reclaiming_never_drops_the_segment_being_written() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        writer.write_all(&[record(0, b"live")]).unwrap();

        assert_eq!(writer.reclaim(u64::MAX).unwrap(), 0);
        assert_eq!(replay_of(&storage).len(), 1);
    }

    #[test]
    fn a_failing_sync_surfaces_as_an_error() {
        let storage = InMemoryStorage::new();
        let mut writer = writer(&storage);
        storage.inject(Fault::Fail(std::io::ErrorKind::StorageFull));

        assert!(writer.write_all(&[record(0, b"doomed")]).is_err());
    }

    // ---- the concurrent façade ----

    fn wal(storage: &InMemoryStorage) -> Wal {
        Wal::open(Arc::new(storage.clone()), dir(), config()).unwrap()
    }

    #[test]
    fn append_returns_only_once_the_record_is_durable() {
        let storage = InMemoryStorage::new();
        let wal = wal(&storage);
        wal.append(RecordKind::Data, 0, b"durable".to_vec())
            .unwrap();

        // No shutdown, no flush: if append returned before the fsync, the
        // record would not be readable yet.
        assert_eq!(replay_of(&storage), vec![record(0, b"durable")]);
    }

    /// §4: `do_put` acknowledges only after fsync, so every acknowledged write
    /// of every concurrent writer must be present after a crash.
    #[test]
    fn every_acknowledged_write_survives_under_concurrency() {
        const WRITERS: u64 = 8;
        const PER_WRITER: u64 = 50;

        let storage = InMemoryStorage::new();
        let wal = Arc::new(wal(&storage));

        let handles: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let wal = Arc::clone(&wal);
                thread::spawn(move || {
                    for step in 0..PER_WRITER {
                        let seq = writer * PER_WRITER + step;
                        wal.append(RecordKind::Data, seq, vec![writer as u8])
                            .unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("writer panicked");
        }

        let mut seqs: Vec<Seq> = replay_of(&storage).iter().map(|r| r.seq).collect();
        seqs.sort_unstable();
        assert_eq!(seqs, (0..WRITERS * PER_WRITER).collect::<Vec<_>>());
    }

    /// A non-zero commit delay must not change what ends up durable — only how
    /// records are grouped. Asserting on the grouping itself would be timing
    /// dependent, so this asserts only on the outcome.
    #[test]
    fn a_commit_delay_does_not_change_what_is_durable() {
        let storage = InMemoryStorage::new();
        let wal = Wal::open(
            Arc::new(storage.clone()),
            dir(),
            WalConfig {
                segment_bytes: 1024,
                max_commit_delay: Duration::from_millis(1),
            },
        )
        .unwrap();

        for seq in 0..10 {
            wal.append(RecordKind::Data, seq, vec![seq as u8]).unwrap();
        }

        assert_eq!(replay_of(&storage).len(), 10);
    }

    #[test]
    fn a_write_failure_reaches_the_caller_that_asked_for_it() {
        let storage = InMemoryStorage::new();
        let wal = wal(&storage);
        storage.inject(Fault::Fail(std::io::ErrorKind::StorageFull));

        assert!(wal.append(RecordKind::Data, 0, b"doomed".to_vec()).is_err());
        // The writer survives the failure and keeps serving.
        assert!(wal.append(RecordKind::Data, 1, b"fine".to_vec()).is_ok());
    }
}
