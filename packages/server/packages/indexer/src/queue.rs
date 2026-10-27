use {
	super::{Indexer, IndexerState, request},
	futures::future,
	std::{
		collections::{BTreeMap, BTreeSet, VecDeque},
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
};

mod tasks;
#[cfg(test)]
mod tests;

pub(super) mod write;

pub(super) type CheckpointSender =
	tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<tg::Result<()>>>;

pub(crate) type BatchId = [u8; 16];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum Kind {
	Archive,
	Index,
}

#[derive(Clone, Copy)]
pub(super) struct SequenceReservation {
	pub(super) end: u64,
	pub(super) kind: Kind,
}

pub(super) struct Queues {
	archive: Queue,
	index: Queue,
	memory: Memory,
}

struct Memory {
	// The fragments whose payloads can be dropped from memory, keyed by the order in which they became droppable.
	evictable: BTreeMap<u64, (Kind, BatchId, u64)>,

	limit: usize,

	stamp: u64,

	used: usize,
}

pub(super) enum Work {
	Delete(Vec<u64>),
	Process(Assembled),
}

pub(super) struct Assembled {
	payloads: Vec<Payload>,
	sequences: Vec<u64>,
}

enum Payload {
	Cache(u64),
	Memory(bytes::Bytes),
}

struct Queue {
	batches: Batches,

	// The completed ranges have exclusive ends and are disjoint and nonadjacent.
	completed: BTreeMap<u64, u64>,

	read_sequence: u64,

	reservation_pending: bool,

	reserved_sequence_end: u64,

	work: VecDeque<Scheduled>,

	write_sequence: u64,

	// The sequences whose writes to the cache have not finished.
	writes: BTreeSet<u64>,
}

enum Scheduled {
	Delete(Vec<u64>),
	Process(BatchId),
}

#[derive(Default)]
struct Batches {
	active: BTreeMap<BatchId, Batch>,
	by_expires_at: BTreeSet<(tokio::time::Instant, BatchId)>,
	complete: BTreeSet<BatchId>,
	ready: BTreeMap<BatchId, Batch>,
	timed_out: BTreeSet<BatchId>,
}

struct Batch {
	deadline: tokio::time::Instant,
	fragments: BTreeMap<u64, Fragment>,
	len: u64,
}

struct Fragment {
	payload: Option<bytes::Bytes>,
	sequences: Vec<u64>,
	stamp: Option<u64>,
	written: Option<u64>,
}

impl Queues {
	#[must_use]
	pub fn empty(memory_limit: usize) -> Self {
		Self {
			archive: Queue::new(0, 0),
			index: Queue::new(0, 0),
			memory: Memory::new(memory_limit),
		}
	}

	#[must_use]
	pub fn new(indexer: &tangram_index::indexer::Indexer, memory_limit: usize) -> Self {
		let archive = Queue::new(
			indexer.archive_read_sequence,
			indexer.archive_write_sequence,
		);
		let index = Queue::new(indexer.index_read_sequence, indexer.index_write_sequence);
		Self {
			archive,
			index,
			memory: Memory::new(memory_limit),
		}
	}

	#[tracing::instrument(name = "indexer.recover_queues", level = "info", skip_all, fields(indexer = %indexer.id()), err(level = "debug"))]
	pub async fn recover(&mut self, indexer: &Indexer) -> tg::Result<()> {
		for kind in [Kind::Archive, Kind::Index] {
			self.recover_queue(indexer, kind).await?;
		}

		Ok(())
	}

	async fn recover_queue(&mut self, indexer: &Indexer, kind: Kind) -> tg::Result<()> {
		let queue = self.queue_mut(kind);
		let start = queue.read_sequence;
		let end = queue.reserved_sequence_end;
		tracing::debug!(?kind, start, end, "recovering the queue");
		let batch_size = indexer.server.config.object.queue_recovery_batch_size;
		let timeout = indexer.batch_timeout(kind);
		let mut sequence_start = start;
		while sequence_start < end {
			let sequence_end = sequence_start.saturating_add(batch_size).min(end);

			// Read the queue items.
			let items = indexer
				.get_queue_items(kind, sequence_start, sequence_end)
				.await
				.map_err(|error| tg::error!(!error, "failed to recover the queue items"))?;
			let bytes = items
				.iter()
				.map(|(_, fragment)| fragment.payload.len())
				.sum::<usize>();
			tracing::debug!(
				?kind,
				bytes,
				count = items.len(),
				sequence_start,
				sequence_end,
				"recovered the queue items"
			);

			// Complete the absent sequences and assemble the present ones.
			let sequences = items
				.iter()
				.map(|(sequence, _)| *sequence)
				.collect::<BTreeSet<_>>();
			for sequence in sequence_start..sequence_end {
				if !sequences.contains(&sequence) {
					self.queue_mut(kind).complete(sequence);
				}
			}
			for (sequence, fragment) in &items {
				if let Err(error) = self.insert_fragment(kind, *sequence, fragment, timeout) {
					tracing::error!(error = %error.trace(), ?kind, sequence, "discarding a recovered queue item");
				}
			}

			sequence_start = sequence_end;
		}

		Ok(())
	}

	pub async fn reserve_initial_sequences(&mut self, indexer: &Indexer) -> tg::Result<()> {
		for kind in [Kind::Archive, Kind::Index] {
			let reservation = self
				.start_reservation(indexer, kind)?
				.ok_or_else(|| tg::error!("failed to prepare an queue reservation"))?;
			indexer.persist_reservation(reservation).await?;
			self.finish_reservation(reservation);
		}

		Ok(())
	}

	#[must_use]
	pub fn try_allocate_sequence(&mut self, kind: Kind) -> Option<u64> {
		let queue = self.queue_mut(kind);
		if queue.write_sequence == queue.reserved_sequence_end {
			return None;
		}
		let sequence = queue.write_sequence;
		queue.write_sequence += 1;
		queue.writes.insert(sequence);

		Some(sequence)
	}

	pub fn insert_fragment(
		&mut self,
		kind: Kind,
		sequence: u64,
		fragment: &request::Fragment,
		batch_timeout: std::time::Duration,
	) -> tg::Result<()> {
		let (queue, memory) = self.queue_and_memory_mut(kind);
		let id = fragment.batch;

		// Delete a fragment of a batch that has already completed or timed out.
		if queue.batches.complete.contains(&id) {
			queue.work.push_back(Scheduled::Delete(vec![sequence]));
			return Ok(());
		}
		if queue.batches.timed_out.contains(&id) {
			queue.work.push_back(Scheduled::Delete(vec![sequence]));
			return Err(tg::error!("the queue batch timed out"));
		}

		// Validate the fragment against the batch.
		if fragment.fragments == 0 || fragment.fragment >= fragment.fragments {
			queue.work.push_back(Scheduled::Delete(vec![sequence]));
			return Err(tg::error!("invalid queue fragment count or position"));
		}
		let batch = queue.batches.active.entry(id).or_insert_with(|| {
			let deadline = tokio::time::Instant::now() + batch_timeout;
			queue.batches.by_expires_at.insert((deadline, id));
			Batch {
				deadline,
				fragments: BTreeMap::new(),
				len: fragment.fragments,
			}
		});
		if batch.len != fragment.fragments {
			queue.work.push_back(Scheduled::Delete(vec![sequence]));
			return Err(tg::error!("conflicting queue fragment counts"));
		}

		// Record the fragment, holding its payload in memory.
		let written = (!queue.writes.contains(&sequence)).then_some(sequence);
		if let Some(existing) = batch.fragments.get_mut(&fragment.fragment) {
			existing.sequences.push(sequence);
			if existing.written.is_none() {
				existing.written = written;
			}
			memory.make_evictable((kind, id, fragment.fragment), existing);
		} else {
			let mut entry = Fragment {
				payload: Some(fragment.payload.clone()),
				sequences: vec![sequence],
				stamp: None,
				written,
			};
			memory.used += fragment.payload.len();
			memory.make_evictable((kind, id, fragment.fragment), &mut entry);
			batch.fragments.insert(fragment.fragment, entry);
		}

		// Schedule the batch for processing once every fragment is present.
		let complete = u64::try_from(batch.fragments.len()).ok() == Some(batch.len);
		if complete {
			let batch = queue.batches.active.remove(&id).unwrap();
			queue.batches.by_expires_at.remove(&(batch.deadline, id));
			let expires_at = tokio::time::Instant::now() + batch_timeout;
			queue.batches.by_expires_at.insert((expires_at, id));
			queue.batches.complete.insert(id);
			queue.batches.ready.insert(id, batch);
			queue.work.push_back(Scheduled::Process(id));
		}

		self.evict();

		Ok(())
	}

	pub fn finish_write(
		&mut self,
		kind: Kind,
		sequence: u64,
		fragment: &request::Fragment,
		succeeded: bool,
	) {
		let (queue, memory) = self.queue_and_memory_mut(kind);
		queue.writes.remove(&sequence);
		if !succeeded {
			return;
		}

		// Record the write, so that the fragment's payload can be read from the cache if it is dropped from memory.
		let Some(entry) = find_fragment(&mut queue.batches, fragment.batch, fragment.fragment)
		else {
			return;
		};
		if !entry.sequences.contains(&sequence) || entry.written.is_some() {
			return;
		}
		entry.written = Some(sequence);
		memory.make_evictable((kind, fragment.batch, fragment.fragment), entry);
		self.evict();
	}

	#[must_use]
	pub fn writes_finished(&self, kind: Kind, sequences: &[u64]) -> bool {
		let queue = self.queue(kind);
		sequences
			.iter()
			.all(|sequence| !queue.writes.contains(sequence))
	}

	pub fn pop_work(&mut self, kind: Kind) -> Option<Work> {
		let (queue, memory) = self.queue_and_memory_mut(kind);
		let work = match queue.work.pop_front()? {
			Scheduled::Delete(sequences) => Work::Delete(sequences),
			Scheduled::Process(id) => {
				let batch = queue.batches.ready.remove(&id).unwrap();
				let mut payloads = Vec::with_capacity(batch.fragments.len());
				let mut sequences = Vec::new();
				for mut fragment in batch.fragments.into_values() {
					memory.release_fragment(&mut fragment);
					let payload = match fragment.payload {
						Some(payload) => Payload::Memory(payload),
						None => Payload::Cache(fragment.written.unwrap()),
					};
					payloads.push(payload);
					sequences.extend(fragment.sequences);
				}
				Work::Process(Assembled {
					payloads,
					sequences,
				})
			},
		};

		Some(work)
	}

	pub fn expire_batches(&mut self, kind: Kind, batch_timeout: std::time::Duration) {
		let (queue, memory) = self.queue_and_memory_mut(kind);
		let now = tokio::time::Instant::now();
		while let Some(&(expires_at, id)) = queue.batches.by_expires_at.first() {
			if expires_at > now {
				break;
			}
			queue.batches.by_expires_at.pop_first();
			if queue.batches.complete.remove(&id) || queue.batches.timed_out.remove(&id) {
				continue;
			}
			let batch = queue.batches.active.remove(&id).unwrap();
			queue
				.batches
				.by_expires_at
				.insert((now + batch_timeout, id));
			queue.batches.timed_out.insert(id);
			let sequences = memory.release_batch(batch);
			queue.work.push_back(Scheduled::Delete(sequences));
		}
	}

	#[must_use]
	pub fn next_batch_deadline(&self) -> Option<tokio::time::Instant> {
		[&self.archive, &self.index]
			.into_iter()
			.filter_map(|queue| {
				queue
					.batches
					.by_expires_at
					.first()
					.map(|(expires_at, _)| *expires_at)
			})
			.min()
	}

	pub fn abandon_incomplete_batches(&mut self) {
		for queue in [&mut self.archive, &mut self.index] {
			let active = std::mem::take(&mut queue.batches.active);
			for (id, batch) in active {
				queue.batches.by_expires_at.remove(&(batch.deadline, id));
				let sequences = self.memory.release_batch(batch);
				queue.work.push_back(Scheduled::Delete(sequences));
			}
		}
	}

	pub fn start_reservation(
		&mut self,
		indexer: &Indexer,
		kind: Kind,
	) -> tg::Result<Option<SequenceReservation>> {
		let reservation_size = match kind {
			Kind::Archive => {
				indexer
					.server
					.config
					.object
					.archive_queue
					.sequence_reservation_size
			},
			Kind::Index => {
				indexer
					.server
					.config
					.object
					.index_queue
					.sequence_reservation_size
			},
		};
		let queue = self.queue_mut(kind);
		let remaining = queue
			.reserved_sequence_end
			.saturating_sub(queue.write_sequence);
		if queue.reservation_pending || remaining > reservation_size / 2 {
			return Ok(None);
		}
		let end = queue
			.reserved_sequence_end
			.checked_add(reservation_size)
			.filter(|value| i64::try_from(*value).is_ok())
			.ok_or_else(|| tg::error!("the queue sequence was exhausted"))?;
		queue.reservation_pending = true;
		let reservation = SequenceReservation { end, kind };

		Ok(Some(reservation))
	}

	#[must_use]
	pub fn reservations_pending(&self) -> bool {
		self.archive.reservation_pending || self.index.reservation_pending
	}

	pub fn finish_reservation(&mut self, reservation: SequenceReservation) {
		let queue = self.queue_mut(reservation.kind);
		queue.reservation_pending = false;
		queue.reserved_sequence_end = reservation.end;
	}

	pub fn complete(&mut self, kind: Kind, sequences: &[u64]) {
		let queue = self.queue_mut(kind);
		for &sequence in sequences {
			queue.complete(sequence);
		}
	}

	#[must_use]
	pub fn drained(&self, archive_target_sequence: u64, index_target_sequence: u64) -> bool {
		self.archive.read_sequence >= archive_target_sequence
			&& self.index.read_sequence >= index_target_sequence
	}

	#[must_use]
	pub fn read_sequences(&self) -> (u64, u64) {
		(self.archive.read_sequence, self.index.read_sequence)
	}

	#[must_use]
	pub fn target_sequences(&self) -> (u64, u64) {
		(self.archive.write_sequence, self.index.write_sequence)
	}

	fn evict(&mut self) {
		// Drop the most recently droppable payloads, because their batches are processed last.
		while self.memory.used > self.memory.limit {
			let Some((_, (kind, id, index))) = self.memory.evictable.pop_last() else {
				break;
			};
			let (queue, memory) = self.queue_and_memory_mut(kind);
			let Some(fragment) = find_fragment(&mut queue.batches, id, index) else {
				continue;
			};
			fragment.stamp = None;
			if let Some(payload) = fragment.payload.take() {
				memory.used -= payload.len();
			}
		}
	}

	fn queue(&self, kind: Kind) -> &Queue {
		match kind {
			Kind::Archive => &self.archive,
			Kind::Index => &self.index,
		}
	}

	fn queue_mut(&mut self, kind: Kind) -> &mut Queue {
		match kind {
			Kind::Archive => &mut self.archive,
			Kind::Index => &mut self.index,
		}
	}

	fn queue_and_memory_mut(&mut self, kind: Kind) -> (&mut Queue, &mut Memory) {
		let queue = match kind {
			Kind::Archive => &mut self.archive,
			Kind::Index => &mut self.index,
		};
		(queue, &mut self.memory)
	}
}

impl Memory {
	fn new(limit: usize) -> Self {
		Self {
			evictable: BTreeMap::new(),
			limit,
			stamp: 0,
			used: 0,
		}
	}

	fn make_evictable(&mut self, location: (Kind, BatchId, u64), fragment: &mut Fragment) {
		// Only a fragment whose write has finished can be dropped from memory, so that it can always be read from the cache.
		if fragment.payload.is_none() || fragment.written.is_none() || fragment.stamp.is_some() {
			return;
		}
		self.stamp += 1;
		fragment.stamp = Some(self.stamp);
		self.evictable.insert(self.stamp, location);
	}

	fn release_fragment(&mut self, fragment: &mut Fragment) {
		if let Some(stamp) = fragment.stamp.take() {
			self.evictable.remove(&stamp);
		}
		if let Some(payload) = &fragment.payload {
			self.used -= payload.len();
		}
	}

	fn release_batch(&mut self, batch: Batch) -> Vec<u64> {
		let mut sequences = Vec::new();
		for mut fragment in batch.fragments.into_values() {
			self.release_fragment(&mut fragment);
			sequences.extend(fragment.sequences);
		}
		sequences
	}
}

impl Queue {
	#[must_use]
	fn new(read_sequence: u64, write_sequence: u64) -> Self {
		Self {
			batches: Batches::default(),
			completed: BTreeMap::new(),
			read_sequence,
			reservation_pending: false,
			reserved_sequence_end: write_sequence,
			work: VecDeque::new(),
			write_sequence,
			writes: BTreeSet::new(),
		}
	}

	fn complete(&mut self, sequence: u64) {
		if sequence < self.read_sequence {
			return;
		}

		// Merge the following range, then extend an existing range or advance the read sequence.
		let end = sequence + 1;
		let end = self.completed.remove(&end).unwrap_or(end);
		if let Some((_, previous_end)) = self.completed.range_mut(..=sequence).next_back()
			&& *previous_end >= sequence
		{
			*previous_end = (*previous_end).max(end);
		} else if sequence == self.read_sequence {
			self.read_sequence = end;
		} else {
			self.completed.insert(sequence, end);
		}
	}
}

impl Indexer {
	pub(super) async fn checkpoint_read_sequences(
		&self,
		archive_read_sequence: u64,
		index_read_sequence: u64,
	) -> tg::Result<()> {
		let arg = tangram_index::indexer::update::Arg {
			id: self.id().clone(),
			value: tangram_index::indexer::update::Value::ArchiveReadSequence(
				archive_read_sequence,
			),
		};
		self.server.index.update_indexer(arg).await?;
		let arg = tangram_index::indexer::update::Arg {
			id: self.id().clone(),
			value: tangram_index::indexer::update::Value::IndexReadSequence(index_read_sequence),
		};
		self.server.index.update_indexer(arg).await?;

		Ok(())
	}

	pub(super) async fn persist_reservation_with_retry(
		&self,
		reservation: SequenceReservation,
	) -> tg::Result<()> {
		tangram_futures::retry(&self.retry_options(), || async {
			match self.persist_reservation(reservation).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to reserve queue sequences");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn persist_reservation(&self, reservation: SequenceReservation) -> tg::Result<()> {
		let value = match reservation.kind {
			Kind::Archive => {
				tangram_index::indexer::update::Value::ArchiveWriteSequence(reservation.end)
			},
			Kind::Index => {
				tangram_index::indexer::update::Value::IndexWriteSequence(reservation.end)
			},
		};
		let arg = tangram_index::indexer::update::Arg {
			id: self.id().clone(),
			value,
		};
		self.server
			.index
			.update_indexer(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to reserve queue sequences"))?;

		Ok(())
	}

	pub(super) async fn queue_task(
		&self,
		kind: Kind,
		state: &Arc<Mutex<IndexerState>>,
		changed: &Arc<tokio::sync::Notify>,
		stopper: Stopper,
	) -> tg::Result<()> {
		if self.id.is_none() {
			stopper.wait().await;
			return Ok(());
		}
		let concurrency = match kind {
			Kind::Archive => self.server.config.object.archive_queue.concurrency,
			Kind::Index => self.server.config.object.index_queue.concurrency,
		};
		let mut stopped = false;
		let mut tasks = tokio::task::JoinSet::new();
		loop {
			let notified = changed.notified();

			// Process the scheduled work up to the concurrency.
			while !stopped && tasks.len() < concurrency {
				let work = state.lock().unwrap().queues.pop_work(kind);
				let Some(work) = work else {
					break;
				};
				tasks.spawn({
					let indexer = self.clone();
					let state = state.clone();
					let changed = changed.clone();
					async move { indexer.process_work(kind, &state, &changed, work).await }
				});
			}

			if stopped && tasks.is_empty() {
				break;
			}
			tokio::select! {
				() = stopper.wait(), if !stopped => {
					stopped = true;
				},
				Some(result) = tasks.join_next() => {
					result.map_err(|source| tg::error!(!source, "a queue task panicked"))??;
				},
				() = notified => {},
			}
		}

		Ok(())
	}

	async fn process_work(
		&self,
		kind: Kind,
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		work: Work,
	) -> tg::Result<()> {
		let sequences = match work {
			Work::Delete(sequences) => sequences,
			Work::Process(batch) => {
				self.process_batch_with_retry(kind, &batch.payloads).await?;
				batch.sequences
			},
		};

		// Wait for the writes to finish, so that a write cannot recreate a deleted queue item.
		loop {
			let notified = changed.notified();
			if state
				.lock()
				.unwrap()
				.queues
				.writes_finished(kind, &sequences)
			{
				break;
			}
			notified.await;
		}

		// Delete the queue items and complete the sequences.
		self.delete_queue_items(kind, &sequences).await?;
		state.lock().unwrap().queues.complete(kind, &sequences);
		changed.notify_waiters();

		Ok(())
	}

	async fn process_batch_with_retry(&self, kind: Kind, payloads: &[Payload]) -> tg::Result<()> {
		let result = tangram_futures::retry(&self.retry_options(), || async {
			let result = match self.read_batch(kind, payloads).await {
				Ok(Some(bytes)) => match kind {
					Kind::Archive => self.process_archive_batch(&bytes).await,
					Kind::Index => self.process_index_batch(&bytes).await,
				},
				Ok(None) => {
					tracing::error!(
						?kind,
						"discarding a queue batch because a fragment is absent from the cache"
					);
					Ok(Ok(()))
				},
				Err(error) => Err(error),
			};
			match result {
				Ok(result) => Ok(ControlFlow::Break(result)),
				Err(error) => {
					tracing::error!(error = %error.trace(), ?kind, "failed to process a queue batch");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		if let Err(error) = result {
			tracing::error!(error = %error.trace(), ?kind, "discarding a rejected queue batch");
		}

		Ok(())
	}

	async fn read_batch(&self, kind: Kind, payloads: &[Payload]) -> tg::Result<Option<Vec<u8>>> {
		// Read the payloads that were dropped from memory from the cache.
		let payloads = future::try_join_all(payloads.iter().map(|payload| async move {
			match payload {
				Payload::Cache(sequence) => self.try_get_queue_payload(kind, *sequence).await,
				Payload::Memory(payload) => Ok(Some(payload.clone())),
			}
		}))
		.await?;
		let Some(payloads) = payloads.into_iter().collect::<Option<Vec<_>>>() else {
			return Ok(None);
		};

		// Reassemble the encoded batch in fragment order.
		let len = payloads.iter().map(bytes::Bytes::len).sum();
		let mut bytes = Vec::with_capacity(len);
		for payload in &payloads {
			bytes.extend_from_slice(payload);
		}

		Ok(Some(bytes))
	}

	#[tracing::instrument(name = "archive.process_batch", level = "debug", skip_all, fields(indexer = %self.id(), bytes = bytes.len()), err(level = "debug"))]
	async fn process_archive_batch(&self, bytes: &[u8]) -> tg::Result<tg::Result<()>> {
		let arg = tangram_server_core::archive::batch::Arg::deserialize(bytes)?;
		tracing::debug!(items = arg.items.len(), "decoded the archive batch");

		// Wait for the object puts and read their bytes from the cache.
		let puts = arg
			.items
			.iter()
			.map(|item| (item.object.clone(), item.put))
			.collect::<Vec<_>>();
		let objects = self
			.try_wait_for_object_put_batch(&self.server.config.object.archive_queue.retry, &puts)
			.await?;
		let mut args = Vec::with_capacity(puts.len());
		for ((id, put), object) in puts.into_iter().zip(objects) {
			let Some(bytes) = object.and_then(|object| object.bytes) else {
				tracing::error!(object = %id, ?put, "discarding an archive item because the object put is absent from the cache");
				continue;
			};
			let arg = tangram_archive::object::put::Arg {
				bytes: bytes.into_owned().into(),
				id,
				put,
			};
			args.push(arg);
		}

		// Archive the objects.
		self.server
			.components
			.object
			.archive_object_batch(&self.server, args)
			.await?;

		Ok(Ok(()))
	}

	#[tracing::instrument(name = "index.process_batch", level = "debug", skip_all, fields(indexer = %self.id(), bytes = bytes.len()), err(level = "debug"))]
	async fn process_index_batch(&self, bytes: &[u8]) -> tg::Result<tg::Result<()>> {
		let arg = tangram_index::batch::Arg::deserialize(bytes)?;
		tracing::debug!(items = arg.items.len(), "decoded the index batch");

		// Wait for the object puts before indexing the batch.
		let puts = arg
			.items
			.iter()
			.filter_map(|item| match item {
				tangram_index::batch::Item::PutObject(arg) => Some((arg.id.clone(), arg.put)),
				_ => None,
			})
			.collect::<BTreeSet<_>>();
		let missing = self
			.wait_for_object_put_batch(&self.server.config.object.index_queue.retry, puts)
			.await?;
		if let Some((id, put)) = missing.first() {
			tracing::error!(%id, ?put, missing_count = missing.len(), "discarding an index queue batch because an object put is absent from the cache");
			return Ok(Ok(()));
		}
		tangram_server_core::checkpoint!(self.server, "index.batch").await;
		let result = self.component.index_batch_inner(&self.server, arg).await?;

		Ok(result)
	}

	fn batch_timeout(&self, kind: Kind) -> std::time::Duration {
		match kind {
			Kind::Archive => self.server.config.object.archive_queue.batch_timeout,
			Kind::Index => self.server.config.object.index_queue.batch_timeout,
		}
	}

	async fn get_queue_items(
		&self,
		kind: Kind,
		sequence_start: u64,
		sequence_end: u64,
	) -> tg::Result<Vec<(u64, request::Fragment)>> {
		let indexer = self.id().clone();
		let items = match kind {
			Kind::Archive => {
				let arg = tangram_server_core::cache::archive::queue::get::batch::Arg {
					indexer,
					sequence_end,
					sequence_start,
				};
				self.server
					.cache
					.get_archive_queue_items(arg)
					.await?
					.into_iter()
					.map(|item| {
						let fragment = request::Fragment {
							batch: item.batch.value(),
							fragment: item.fragment,
							fragments: item.fragments,
							payload: item.payload,
						};
						(item.sequence, fragment)
					})
					.collect()
			},
			Kind::Index => {
				let arg = tangram_server_core::cache::index::queue::get::batch::Arg {
					indexer,
					sequence_end,
					sequence_start,
				};
				self.server
					.cache
					.get_index_queue_items(arg)
					.await?
					.into_iter()
					.map(|item| {
						let fragment = request::Fragment {
							batch: item.batch.value(),
							fragment: item.fragment,
							fragments: item.fragments,
							payload: item.payload,
						};
						(item.sequence, fragment)
					})
					.collect()
			},
		};

		Ok(items)
	}

	async fn try_get_queue_payload(
		&self,
		kind: Kind,
		sequence: u64,
	) -> tg::Result<Option<bytes::Bytes>> {
		let indexer = self.id().clone();
		let payload = match kind {
			Kind::Archive => {
				let arg =
					tangram_server_core::cache::archive::queue::get::Arg { indexer, sequence };
				self.server
					.cache
					.try_get_archive_queue_item(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to get an archive queue item"))?
					.map(|item| item.payload)
			},
			Kind::Index => {
				let arg = tangram_server_core::cache::index::queue::get::Arg { indexer, sequence };
				self.server
					.cache
					.try_get_index_queue_item(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to get an index queue item"))?
					.map(|item| item.payload)
			},
		};

		Ok(payload)
	}

	pub(super) async fn put_queue_item(
		&self,
		kind: Kind,
		sequence: u64,
		fragment: &request::Fragment,
	) -> tg::Result<()> {
		let indexer = self.id().clone();
		match kind {
			Kind::Archive => {
				let item = tangram_server_core::cache::archive::queue::Item {
					batch: tangram_server_core::cache::archive::queue::batch::Id::new(
						fragment.batch,
					),
					fragment: fragment.fragment,
					fragments: fragment.fragments,
					indexer,
					payload: fragment.payload.clone(),
					sequence,
				};
				let arg = tangram_server_core::cache::archive::queue::put::Arg { item };
				self.server
					.cache
					.put_archive_queue_item(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to put an archive queue item"))?;
			},
			Kind::Index => {
				let item = tangram_server_core::cache::index::queue::Item {
					batch: tangram_server_core::cache::index::queue::batch::Id::new(fragment.batch),
					fragment: fragment.fragment,
					fragments: fragment.fragments,
					indexer,
					payload: fragment.payload.clone(),
					sequence,
				};
				let arg = tangram_server_core::cache::index::queue::put::Arg { item };
				self.server
					.cache
					.put_index_queue_item(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to put an index queue item"))?;
			},
		}

		Ok(())
	}

	async fn delete_queue_items(&self, kind: Kind, sequences: &[u64]) -> tg::Result<()> {
		tangram_futures::retry(&self.retry_options(), || async {
			match self.delete_queue_item_batch(kind, sequences).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), ?kind, "failed to delete the queue items");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn delete_queue_item_batch(&self, kind: Kind, sequences: &[u64]) -> tg::Result<()> {
		let indexer = self.id().clone();
		let sequences = sequences.to_vec();
		match kind {
			Kind::Archive => {
				let arg = tangram_server_core::cache::archive::queue::delete::batch::Arg {
					indexer,
					sequences,
				};
				self.server
					.cache
					.delete_archive_queue_item_batch(arg)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to delete the archive queue items")
					})?;
			},
			Kind::Index => {
				let arg = tangram_server_core::cache::index::queue::delete::batch::Arg {
					indexer,
					sequences,
				};
				self.server
					.cache
					.delete_index_queue_item_batch(arg)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to delete the index queue items")
					})?;
			},
		}

		Ok(())
	}
}

fn find_fragment(batches: &mut Batches, id: BatchId, index: u64) -> Option<&mut Fragment> {
	let batch = if let Some(batch) = batches.active.get_mut(&id) {
		batch
	} else {
		batches.ready.get_mut(&id)?
	};
	batch.fragments.get_mut(&index)
}
