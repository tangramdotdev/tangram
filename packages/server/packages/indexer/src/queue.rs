use {
	super::{Indexer, IndexerState},
	std::{
		collections::{BTreeMap, BTreeSet},
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
	// The entries whose payloads can be dropped from memory, keyed by the order in which they became droppable.
	evictable: BTreeMap<u64, (Kind, u64)>,

	limit: usize,

	stamp: u64,

	used: usize,
}

pub(super) struct Work {
	payload: Payload,
	pub(super) sequence: u64,
}

enum Payload {
	Cache,
	Memory(bytes::Bytes),
}

struct Queue {
	// The completed ranges have exclusive ends and are disjoint and nonadjacent.
	completed: BTreeMap<u64, u64>,

	// The entries that have not been popped, keyed by sequence.
	entries: BTreeMap<u64, Entry>,

	read_sequence: u64,

	reservation_pending: bool,

	reserved_sequence_end: u64,

	write_sequence: u64,

	// The sequences whose writes to the cache have not finished.
	writes: BTreeSet<u64>,
}

struct Entry {
	payload: Option<bytes::Bytes>,
	stamp: Option<u64>,
	written: bool,
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
				.map(|(_, payload)| payload.len())
				.sum::<usize>();
			tracing::debug!(
				?kind,
				bytes,
				count = items.len(),
				sequence_start,
				sequence_end,
				"recovered the queue items"
			);

			// Complete the absent sequences and record each present one as an entry.
			let sequences = items
				.iter()
				.map(|(sequence, _)| *sequence)
				.collect::<BTreeSet<_>>();
			for sequence in sequence_start..sequence_end {
				if !sequences.contains(&sequence) {
					self.queue_mut(kind).complete(sequence);
				}
			}
			for (sequence, payload) in items {
				self.insert_entry(kind, sequence, payload);
			}

			sequence_start = sequence_end;
		}

		Ok(())
	}

	pub async fn reserve_initial_sequences(&mut self, indexer: &Indexer) -> tg::Result<()> {
		for kind in [Kind::Archive, Kind::Index] {
			let reservation = self
				.start_reservation(indexer, kind)?
				.ok_or_else(|| tg::error!("failed to prepare a queue reservation"))?;
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

	pub fn insert_entry(&mut self, kind: Kind, sequence: u64, payload: bytes::Bytes) {
		// Record the entry, holding its payload in memory.
		let (queue, memory) = self.queue_and_memory_mut(kind);
		let written = !queue.writes.contains(&sequence);
		memory.used += payload.len();
		let mut entry = Entry {
			payload: Some(payload),
			stamp: None,
			written,
		};
		memory.make_evictable((kind, sequence), &mut entry);
		queue.entries.insert(sequence, entry);

		self.evict();
	}

	pub fn finish_write(&mut self, kind: Kind, sequence: u64, succeeded: bool) {
		let (queue, memory) = self.queue_and_memory_mut(kind);
		queue.writes.remove(&sequence);
		if !succeeded {
			return;
		}

		// Record the write, so that the entry's payload can be read from the cache if it is dropped from memory.
		let Some(entry) = queue.entries.get_mut(&sequence) else {
			return;
		};
		entry.written = true;
		memory.make_evictable((kind, sequence), entry);

		self.evict();
	}

	#[must_use]
	pub fn write_finished(&self, kind: Kind, sequence: u64) -> bool {
		!self.queue(kind).writes.contains(&sequence)
	}

	pub fn pop_entry(&mut self, kind: Kind) -> Option<Work> {
		let (queue, memory) = self.queue_and_memory_mut(kind);
		let (sequence, mut entry) = queue.entries.pop_first()?;
		memory.release(&mut entry);
		let payload = match entry.payload {
			Some(payload) => Payload::Memory(payload),
			None => Payload::Cache,
		};
		let work = Work { payload, sequence };

		Some(work)
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

	pub fn complete(&mut self, kind: Kind, sequence: u64) {
		self.queue_mut(kind).complete(sequence);
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
		// Drop the most recently droppable payloads, because their entries are processed last.
		while self.memory.used > self.memory.limit {
			let Some((_, (kind, sequence))) = self.memory.evictable.pop_last() else {
				break;
			};
			let (queue, memory) = self.queue_and_memory_mut(kind);
			let Some(entry) = queue.entries.get_mut(&sequence) else {
				continue;
			};
			entry.stamp = None;
			if let Some(payload) = entry.payload.take() {
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

	fn make_evictable(&mut self, location: (Kind, u64), entry: &mut Entry) {
		// Only an entry whose write has finished can be dropped from memory, so that it can always be read from the cache.
		if entry.payload.is_none() || !entry.written || entry.stamp.is_some() {
			return;
		}
		self.stamp += 1;
		entry.stamp = Some(self.stamp);
		self.evictable.insert(self.stamp, location);
	}

	fn release(&mut self, entry: &mut Entry) {
		if let Some(stamp) = entry.stamp.take() {
			self.evictable.remove(&stamp);
		}
		if let Some(payload) = &entry.payload {
			self.used -= payload.len();
		}
	}
}

impl Queue {
	#[must_use]
	fn new(read_sequence: u64, write_sequence: u64) -> Self {
		Self {
			completed: BTreeMap::new(),
			entries: BTreeMap::new(),
			read_sequence,
			reservation_pending: false,
			reserved_sequence_end: write_sequence,
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

			// Process the entries in sequence order up to the concurrency.
			while !stopped && tasks.len() < concurrency {
				let work = state.lock().unwrap().queues.pop_entry(kind);
				let Some(work) = work else {
					break;
				};
				tasks.spawn({
					let indexer = self.clone();
					let state = state.clone();
					let changed = changed.clone();
					async move { indexer.process_entry(kind, &state, &changed, work).await }
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

	async fn process_entry(
		&self,
		kind: Kind,
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		work: Work,
	) -> tg::Result<()> {
		let Work { payload, sequence } = work;

		// Process the entry.
		self.process_entry_with_retry(kind, sequence, &payload)
			.await?;

		// Wait for the write to finish, so that the write cannot recreate the deleted queue item.
		loop {
			let notified = changed.notified();
			if state.lock().unwrap().queues.write_finished(kind, sequence) {
				break;
			}
			notified.await;
		}

		// Delete the queue item and complete the sequence.
		self.delete_queue_item(kind, sequence).await?;
		state.lock().unwrap().queues.complete(kind, sequence);
		changed.notify_waiters();

		Ok(())
	}

	async fn process_entry_with_retry(
		&self,
		kind: Kind,
		sequence: u64,
		payload: &Payload,
	) -> tg::Result<()> {
		let result = tangram_futures::retry(&self.retry_options(), || async {
			let result = match self.read_entry(kind, sequence, payload).await {
				Ok(Some(bytes)) => match kind {
					Kind::Archive => self.process_archive_entry(&bytes).await,
					Kind::Index => self.process_index_entry(&bytes).await,
				},
				Ok(None) => {
					tracing::error!(
						?kind,
						sequence,
						"discarding a queue entry because it is absent from the cache"
					);
					Ok(Ok(()))
				},
				Err(error) => Err(error),
			};
			match result {
				Ok(result) => Ok(ControlFlow::Break(result)),
				Err(error) => {
					tracing::error!(error = %error.trace(), ?kind, sequence, "failed to process a queue entry");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		if let Err(error) = result {
			tracing::error!(error = %error.trace(), ?kind, sequence, "discarding a rejected queue entry");
		}

		Ok(())
	}

	async fn read_entry(
		&self,
		kind: Kind,
		sequence: u64,
		payload: &Payload,
	) -> tg::Result<Option<bytes::Bytes>> {
		// Read a payload that was dropped from memory from the cache.
		let payload = match payload {
			Payload::Cache => self.try_get_queue_payload(kind, sequence).await?,
			Payload::Memory(payload) => Some(payload.clone()),
		};

		Ok(payload)
	}

	#[tracing::instrument(name = "archive.process_entry", level = "debug", skip_all, fields(indexer = %self.id(), bytes = bytes.len()), err(level = "debug"))]
	async fn process_archive_entry(&self, bytes: &[u8]) -> tg::Result<tg::Result<()>> {
		let arg = tangram_server_core::archive::batch::Arg::deserialize(bytes)?;
		tracing::debug!(items = arg.items.len(), "decoded the archive entry");

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

	#[tracing::instrument(name = "index.process_entry", level = "debug", skip_all, fields(indexer = %self.id(), bytes = bytes.len()), err(level = "debug"))]
	async fn process_index_entry(&self, bytes: &[u8]) -> tg::Result<tg::Result<()>> {
		let arg = tangram_index::write::Arg::deserialize(bytes)?;
		tracing::debug!(items = arg.items.len(), "decoded the index entry");

		// Wait for the object puts before writing the items.
		let puts = arg
			.items
			.iter()
			.filter_map(|item| match item {
				tangram_index::Item::PutObject(arg) => Some((arg.id.clone(), arg.put)),
				_ => None,
			})
			.collect::<BTreeSet<_>>();
		let missing = self
			.wait_for_object_put_batch(&self.server.config.object.index_queue.retry, puts)
			.await?;
		if let Some((id, put)) = missing.first() {
			tracing::error!(%id, ?put, missing_count = missing.len(), "discarding an index queue entry because an object put is absent from the cache");
			return Ok(Ok(()));
		}
		tangram_server_core::checkpoint!(self.server, "index.write").await;
		let result = self.component.write_inner(&self.server, arg).await?;

		Ok(result)
	}

	async fn get_queue_items(
		&self,
		kind: Kind,
		sequence_start: u64,
		sequence_end: u64,
	) -> tg::Result<Vec<(u64, bytes::Bytes)>> {
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
					.map(|item| (item.sequence, item.payload))
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
					.map(|item| (item.sequence, item.payload))
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
		payload: &bytes::Bytes,
	) -> tg::Result<()> {
		let indexer = self.id().clone();
		let payload = payload.clone();
		match kind {
			Kind::Archive => {
				let item = tangram_server_core::cache::archive::queue::Item {
					indexer,
					payload,
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
					indexer,
					payload,
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

	async fn delete_queue_item(&self, kind: Kind, sequence: u64) -> tg::Result<()> {
		tangram_futures::retry(&self.retry_options(), || async {
			match self.delete_queue_item_inner(kind, sequence).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), ?kind, sequence, "failed to delete the queue item");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn delete_queue_item_inner(&self, kind: Kind, sequence: u64) -> tg::Result<()> {
		let indexer = self.id().clone();
		let sequences = vec![sequence];
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
						tg::error!(!error, "failed to delete the archive queue item")
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
					.map_err(|error| tg::error!(!error, "failed to delete the index queue item"))?;
			},
		}

		Ok(())
	}
}
