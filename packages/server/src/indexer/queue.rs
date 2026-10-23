use {
	super::{Indexer, State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
		sync::Mutex,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tokio_stream::wrappers::ReceiverStream,
};

mod tasks;

pub(super) type CheckpointSender =
	tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<tg::Result<()>>>;

pub(super) type CompletionReceiver = tokio::sync::mpsc::UnboundedReceiver<Completion>;
pub(super) type CompletionSender = tokio::sync::mpsc::UnboundedSender<Completion>;
pub(super) type ArchiveMessageSender = tokio::sync::mpsc::Sender<ArchiveMessage>;
pub(super) type IndexMessageSender = tokio::sync::mpsc::Sender<IndexMessage>;

pub(super) struct Output {
	pub messages: Vec<IndexMessage>,
	pub responses: Vec<(tokio::sync::oneshot::Sender<tg::Result<()>>, tg::Result<()>)>,
}

pub(super) enum ArchiveMessage {
	Delete(u64),
	Process(crate::cache::archive::queue::Entry),
}

pub(super) enum Completion {
	Archive(u64),
	Index(Vec<u64>),
}

#[derive(Clone, Copy)]
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
	batches: Batches,
	index: Queue,
}

pub(super) enum IndexMessage {
	Delete(Vec<u64>),
	Process(IndexBatch),
}

struct Queue {
	// The completed and sent ranges have exclusive ends and are disjoint and nonadjacent.
	completed: BTreeMap<u64, u64>,
	deletes: BTreeSet<u64>,
	read_sequence: u64,
	reservation_pending: bool,
	reserved_sequence_end: u64,
	sent: BTreeMap<u64, u64>,
	sent_sequence: u64,
	write_sequence: u64,
}

#[derive(Default)]
struct Batches {
	active: BTreeMap<crate::cache::index::queue::batch::Id, Batch>,
	by_expires_at: BTreeSet<(tokio::time::Instant, crate::cache::index::queue::batch::Id)>,
	complete: BTreeSet<crate::cache::index::queue::batch::Id>,
	timed_out: BTreeSet<crate::cache::index::queue::batch::Id>,
}

struct Batch {
	deadline: tokio::time::Instant,
	fragments: BTreeMap<u64, BatchFragment>,
	len: u64,
	waiters: Vec<tokio::sync::oneshot::Sender<tg::Result<()>>>,
}

struct BatchFragment {
	fragment: crate::cache::index::queue::Fragment,
	sequences: Vec<u64>,
}

pub(super) struct IndexBatch {
	fragments: Vec<crate::cache::index::queue::Fragment>,
	sequences: Vec<u64>,
}

struct ReaderBatch {
	fragments: BTreeMap<u64, crate::cache::index::queue::Fragment>,
	len: u64,
	sequences: Vec<u64>,
}

impl Output {
	#[must_use]
	fn new() -> Self {
		Self {
			messages: Vec::new(),
			responses: Vec::new(),
		}
	}
}

impl Queues {
	#[must_use]
	pub fn empty() -> Self {
		Self {
			archive: Queue::new(0, 0),
			batches: Batches::default(),
			index: Queue::new(0, 0),
		}
	}

	#[must_use]
	pub fn new(indexer: &tangram_index::indexer::Indexer) -> Self {
		let archive = Queue::new(
			indexer.archive_read_sequence,
			indexer.archive_write_sequence,
		);
		let mut index = Queue::new(indexer.index_read_sequence, indexer.index_write_sequence);
		index.sent_sequence = indexer.index_read_sequence;
		Self {
			archive,
			batches: Batches::default(),
			index,
		}
	}

	#[tracing::instrument(name = "indexer.recover_queues", level = "info", skip_all, fields(indexer = %indexer.id(), index_start = self.index.read_sequence, index_end = self.index.reserved_sequence_end), err(level = "debug"))]
	pub async fn recover(&mut self, indexer: &Indexer) -> tg::Result<()> {
		tracing::debug!(
			index_start = self.index.read_sequence,
			index_end = self.index.reserved_sequence_end,
			"recovering the queues"
		);
		let batch_size = indexer.server.config.object.queue_recovery_batch_size;
		let mut sequence_start = self.index.read_sequence;
		while sequence_start < self.index.reserved_sequence_end {
			let sequence_end = sequence_start
				.saturating_add(batch_size)
				.min(self.index.reserved_sequence_end);
			let arg = crate::cache::index::queue::get::batch::Arg {
				indexer: indexer.id().clone(),
				sequence_end,
				sequence_start,
			};
			tracing::debug!(
				sequence_start,
				sequence_end,
				"reading the index queue batch"
			);
			let fragments = indexer
				.server
				.cache
				.get_index_queue_fragments(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to recover index queue fragments"))?;
			let bytes = fragments
				.iter()
				.map(|fragment| fragment.payload.len())
				.sum::<usize>();
			tracing::debug!(
				bytes,
				count = fragments.len(),
				sequence_start,
				sequence_end,
				"recovered the index queue batch"
			);
			let sequences = fragments
				.iter()
				.map(|fragment| fragment.sequence)
				.collect::<BTreeSet<_>>();
			for sequence in sequence_start..sequence_end {
				if !sequences.contains(&sequence) {
					self.mark_sent(Kind::Index, [sequence], false);
				}
			}
			for fragment in fragments {
				let timeout = indexer.server.config.object.index_queue.batch_timeout;
				let output = self.insert_index_fragment(fragment, None, timeout);
				self.mark_sent_index_messages(output.messages);
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

		Some(sequence)
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

	pub fn complete(&mut self, completion: Completion) {
		match completion {
			Completion::Archive(sequence) => self.archive.complete(sequence),
			Completion::Index(sequences) => {
				for sequence in sequences {
					self.index.complete(sequence);
				}
			},
		}
	}

	pub fn mark_sent(&mut self, kind: Kind, sequences: impl IntoIterator<Item = u64>, delete: bool) {
		let queue = self.queue_mut(kind);
		for sequence in sequences {
			if delete {
				queue.deletes.insert(sequence);
			}
			queue.send(sequence);
		}
	}

	pub fn mark_sent_index_messages(&mut self, messages: Vec<IndexMessage>) {
		for message in messages {
			match message {
				IndexMessage::Delete(sequences) => self.mark_sent(Kind::Index, sequences, true),
				IndexMessage::Process(batch) => self.mark_sent(Kind::Index, batch.sequences, false),
			}
		}
	}

	#[must_use]
	pub fn take_sent_sequences(&mut self, kind: Kind) -> (u64, BTreeSet<u64>) {
		let queue = self.queue_mut(kind);
		let end = queue.sent_sequence;
		let pending = queue.deletes.split_off(&end);
		let deletes = std::mem::replace(&mut queue.deletes, pending);
		(end, deletes)
	}

	pub fn insert_index_fragment(
		&mut self,
		fragment: crate::cache::index::queue::Fragment,
		waiter: Option<tokio::sync::oneshot::Sender<tg::Result<()>>>,
		batch_timeout: std::time::Duration,
	) -> Output {
		let mut output = Output::new();
		let batch_id = fragment.batch;
		if self.batches.complete.contains(&batch_id) || self.batches.timed_out.contains(&batch_id) {
			output
				.messages
				.push(IndexMessage::Delete(vec![fragment.sequence]));
			if let Some(waiter) = waiter {
				let result = if self.batches.timed_out.contains(&batch_id) {
					Err(tg::error!("the index queue batch timed out"))
				} else {
					Ok(())
				};
				output.responses.push((waiter, result));
			}

			return output;
		}
		let batch = self.batches.active.entry(batch_id).or_insert_with(|| {
			let deadline = tokio::time::Instant::now() + batch_timeout;
			self.batches.by_expires_at.insert((deadline, batch_id));
			Batch {
				deadline,
				fragments: BTreeMap::new(),
				len: fragment.fragments,
				waiters: Vec::new(),
			}
		});
		if batch.len != fragment.fragments {
			output
				.messages
				.push(IndexMessage::Delete(vec![fragment.sequence]));
			if let Some(waiter) = waiter {
				output.responses.push((
					waiter,
					Err(tg::error!("conflicting index queue fragment counts")),
				));
			}

			return output;
		}
		if let Some(existing) = batch.fragments.get_mut(&fragment.fragment) {
			if existing.fragment.payload != fragment.payload {
				output
					.messages
					.push(IndexMessage::Delete(vec![fragment.sequence]));
				if let Some(waiter) = waiter {
					output.responses.push((
						waiter,
						Err(tg::error!("conflicting index queue fragment payloads")),
					));
				}

				return output;
			}
			existing.sequences.push(fragment.sequence);
		} else {
			let sequence = fragment.sequence;
			batch.fragments.insert(
				fragment.fragment,
				BatchFragment {
					fragment,
					sequences: vec![sequence],
				},
			);
		}
		if let Some(waiter) = waiter {
			batch.waiters.push(waiter);
		}
		let complete = u64::try_from(batch.fragments.len()).ok() == Some(batch.len)
			&& batch.fragments.keys().copied().eq(0..batch.len);
		if !complete {
			return output;
		}
		let batch = self.batches.active.remove(&batch_id).unwrap();
		self.batches
			.by_expires_at
			.remove(&(batch.deadline, batch_id));
		let expires_at = tokio::time::Instant::now() + batch_timeout;
		self.batches.by_expires_at.insert((expires_at, batch_id));
		self.batches.complete.insert(batch_id);
		let mut fragments = Vec::with_capacity(batch.fragments.len());
		let mut sequences = Vec::new();
		for fragment in batch.fragments.into_values() {
			fragments.push(fragment.fragment);
			sequences.extend(fragment.sequences);
		}
		output
			.responses
			.extend(batch.waiters.into_iter().map(|waiter| (waiter, Ok(()))));
		output.messages.push(IndexMessage::Process(IndexBatch {
			fragments,
			sequences,
		}));

		output
	}

	pub fn expire_index_batches(&mut self, batch_timeout: std::time::Duration) -> Output {
		let mut output = Output::new();
		let now = tokio::time::Instant::now();
		while let Some(&(expires_at, id)) = self.batches.by_expires_at.first() {
			if expires_at > now {
				break;
			}
			self.batches.by_expires_at.pop_first();
			if self.batches.complete.remove(&id) || self.batches.timed_out.remove(&id) {
				continue;
			}
			let batch = self.batches.active.remove(&id).unwrap();
			self.batches.by_expires_at.insert((now + batch_timeout, id));
			self.batches.timed_out.insert(id);
			output.responses.extend(
				batch
					.waiters
					.into_iter()
					.map(|waiter| (waiter, Err(tg::error!("the index queue batch timed out")))),
			);
			let sequences = batch
				.fragments
				.into_values()
				.flat_map(|fragment| fragment.sequences)
				.collect();
			output.messages.push(IndexMessage::Delete(sequences));
		}

		output
	}

	#[must_use]
	pub fn next_batch_deadline(&self) -> Option<tokio::time::Instant> {
		self.batches
			.by_expires_at
			.first()
			.map(|(expires_at, _)| *expires_at)
	}

	pub fn abandon_incomplete_batches(&mut self) -> Output {
		let mut output = Output::new();
		let active = std::mem::take(&mut self.batches.active);
		for (id, batch) in active {
			self.batches.by_expires_at.remove(&(batch.deadline, id));
			output.responses.extend(
				batch
					.waiters
					.into_iter()
					.map(|waiter| (waiter, Err(tg::error!("the indexer is shutting down")))),
			);
			let sequences = batch
				.fragments
				.into_values()
				.flat_map(|fragment| fragment.sequences)
				.collect();
			output.messages.push(IndexMessage::Delete(sequences));
		}

		output
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

	fn queue_mut(&mut self, kind: Kind) -> &mut Queue {
		match kind {
			Kind::Archive => &mut self.archive,
			Kind::Index => &mut self.index,
		}
	}
}

impl Queue {
	#[must_use]
	fn new(read_sequence: u64, write_sequence: u64) -> Self {
		Self {
			completed: BTreeMap::new(),
			deletes: BTreeSet::new(),
			read_sequence,
			reservation_pending: false,
			reserved_sequence_end: write_sequence,
			sent: BTreeMap::new(),
			sent_sequence: write_sequence,
			write_sequence,
		}
	}

	fn complete(&mut self, sequence: u64) {
		insert_sequence(&mut self.read_sequence, &mut self.completed, sequence);
	}

	fn send(&mut self, sequence: u64) {
		insert_sequence(&mut self.sent_sequence, &mut self.sent, sequence);
	}
}

fn insert_sequence(contiguous: &mut u64, ranges: &mut BTreeMap<u64, u64>, sequence: u64) {
	if sequence < *contiguous {
		return;
	}

	// Merge the following range, then extend an existing range or advance the contiguous sequence.
	let end = sequence + 1;
	let end = ranges.remove(&end).unwrap_or(end);
	if let Some((_, previous_end)) = ranges.range_mut(..=sequence).next_back()
		&& *previous_end >= sequence
	{
		*previous_end = (*previous_end).max(end);
	} else if sequence == *contiguous {
		*contiguous = end;
	} else {
		ranges.insert(sequence, end);
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

	pub(super) async fn archive_queue_task(
		&self,
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		sender: CompletionSender,
		stopper: Stopper,
	) -> tg::Result<()> {
		if self.id.is_none() {
			stopper.wait().await;
			return Ok(());
		}
		let (message_sender, message_receiver) =
			tokio::sync::mpsc::channel(self.server.config.object.archive_queue.concurrency);
		let reader = self.archive_queue_reader(state, changed, &message_sender, &sender);
		let processor = ReceiverStream::new(message_receiver)
			.take_until(stopper.wait())
			.map(Ok)
			.try_for_each_concurrent(
				self.server.config.object.archive_queue.concurrency,
				|message| {
					let sender = sender.clone();
					async move {
						let completion = self.process_archive_message(message).await?;
						sender
							.send(completion)
							.map_err(|_| tg::error!("the queue completion task stopped"))?;

						Ok(())
					}
				},
			);
		tokio::select! {
			result = reader => result,
			result = processor => result,
		}
	}

	async fn archive_queue_reader(
		&self,
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		sender: &ArchiveMessageSender,
		completions: &CompletionSender,
	) -> tg::Result<()> {
		let batch_size = self.server.config.object.queue_recovery_batch_size;
		let mut sequence_start = state.lock().unwrap().queues.read_sequences().0;
		loop {
			let notified = changed.notified();
			let (sent_end, deletes) = state
				.lock()
				.unwrap()
				.queues
				.take_sent_sequences(Kind::Archive);
			if sequence_start >= sent_end {
				notified.await;
				continue;
			}
			while sequence_start < sent_end {
				let sequence_end = sequence_start.saturating_add(batch_size).min(sent_end);
				let mut entries = self
					.get_archive_queue_entries_with_retry(sequence_start, sequence_end)
					.await?
					.into_iter()
					.map(|entry| (entry.sequence, entry))
					.collect::<BTreeMap<_, _>>();
				for sequence in sequence_start..sequence_end {
					let message = if deletes.contains(&sequence) {
						Some(ArchiveMessage::Delete(sequence))
					} else {
						entries.remove(&sequence).map(ArchiveMessage::Process)
					};
					if let Some(message) = message {
						sender
							.send(message)
							.await
							.map_err(|_| tg::error!("the archive queue task stopped"))?;
					} else {
						completions
							.send(Completion::Archive(sequence))
							.map_err(|_| tg::error!("the queue completion task stopped"))?;
					}
				}
				sequence_start = sequence_end;
			}
		}
	}

	async fn get_archive_queue_entries_with_retry(
		&self,
		sequence_start: u64,
		sequence_end: u64,
	) -> tg::Result<Vec<crate::cache::archive::queue::Entry>> {
		tangram_futures::retry(&self.retry_options(), || async {
			let arg = crate::cache::archive::queue::get::batch::Arg {
				indexer: self.id().clone(),
				sequence_end,
				sequence_start,
			};
			match self.server.cache.get_archive_queue_entries(arg).await {
				Ok(entries) => Ok(ControlFlow::Break(entries)),
				Err(error) => {
					tracing::error!(error = %error.trace(), sequence_start, sequence_end, "failed to read archive queue entries");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await
	}

	pub(super) async fn index_queue_task(
		&self,
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		sender: CompletionSender,
		stopper: Stopper,
	) -> tg::Result<()> {
		if self.id.is_none() {
			stopper.wait().await;
			return Ok(());
		}
		let (message_sender, message_receiver) =
			tokio::sync::mpsc::channel(self.server.config.object.index_queue.concurrency);
		let reader = self.index_queue_reader(state, changed, &message_sender, &sender);
		let processor = ReceiverStream::new(message_receiver)
			.take_until(stopper.wait())
			.map(Ok)
			.try_for_each_concurrent(
				self.server.config.object.index_queue.concurrency,
				|message| {
					let sender = sender.clone();
					async move {
						let completion = self.process_index_message(message).await?;
						sender
							.send(completion)
							.map_err(|_| tg::error!("the queue completion task stopped"))?;

						Ok(())
					}
				},
			);
		tokio::select! {
			result = reader => result,
			result = processor => result,
		}
	}

	async fn index_queue_reader(
		&self,
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		sender: &IndexMessageSender,
		completions: &CompletionSender,
	) -> tg::Result<()> {
		let batch_size = self.server.config.object.queue_recovery_batch_size;
		let mut sequence_start = state.lock().unwrap().queues.read_sequences().1;
		let mut batches = BTreeMap::<_, ReaderBatch>::new();
		loop {
			let notified = changed.notified();
			let (sent_end, deletes) = state
				.lock()
				.unwrap()
				.queues
				.take_sent_sequences(Kind::Index);
			if sequence_start >= sent_end {
				notified.await;
				continue;
			}
			while sequence_start < sent_end {
				let sequence_end = sequence_start.saturating_add(batch_size).min(sent_end);
				let mut fragments = self
					.get_index_queue_fragments_with_retry(sequence_start, sequence_end)
					.await?
					.into_iter()
					.map(|fragment| (fragment.sequence, fragment))
					.collect::<BTreeMap<_, _>>();
				for sequence in sequence_start..sequence_end {
					let message = if deletes.contains(&sequence) {
						Some(IndexMessage::Delete(vec![sequence]))
					} else if let Some(fragment) = fragments.remove(&sequence) {
						let id = fragment.batch;
						let batch = batches.entry(id).or_insert_with(|| ReaderBatch {
							fragments: BTreeMap::new(),
							len: fragment.fragments,
							sequences: Vec::new(),
						});
						batch.sequences.push(sequence);
						batch.fragments.entry(fragment.fragment).or_insert(fragment);
						if u64::try_from(batch.fragments.len()).ok() == Some(batch.len) {
							let batch = batches.remove(&id).unwrap();
							Some(IndexMessage::Process(IndexBatch {
								fragments: batch.fragments.into_values().collect(),
								sequences: batch.sequences,
							}))
						} else {
							continue;
						}
					} else {
						None
					};
					if let Some(message) = message {
						sender
							.send(message)
							.await
							.map_err(|_| tg::error!("the index queue task stopped"))?;
					} else {
						completions
							.send(Completion::Index(vec![sequence]))
							.map_err(|_| tg::error!("the queue completion task stopped"))?;
					}
				}
				sequence_start = sequence_end;
			}
		}
	}

	async fn get_index_queue_fragments_with_retry(
		&self,
		sequence_start: u64,
		sequence_end: u64,
	) -> tg::Result<Vec<crate::cache::index::queue::Fragment>> {
		tangram_futures::retry(&self.retry_options(), || async {
			let arg = crate::cache::index::queue::get::batch::Arg {
				indexer: self.id().clone(),
				sequence_end,
				sequence_start,
			};
			match self.server.cache.get_index_queue_fragments(arg).await {
				Ok(fragments) => Ok(ControlFlow::Break(fragments)),
				Err(error) => {
					tracing::error!(error = %error.trace(), sequence_start, sequence_end, "failed to read index queue fragments");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await
	}

	async fn process_archive_message(&self, message: ArchiveMessage) -> tg::Result<Completion> {
		let completion = match message {
			ArchiveMessage::Delete(sequence) => {
				self.delete_archive_sequence(sequence).await?;

				Completion::Archive(sequence)
			},
			ArchiveMessage::Process(entry) => {
				self.process_archive_entry_with_retry(&entry).await?;
				self.delete_archive_sequence(entry.sequence).await?;

				Completion::Archive(entry.sequence)
			},
		};

		Ok(completion)
	}

	async fn process_index_message(&self, message: IndexMessage) -> tg::Result<Completion> {
		let completion = match message {
			IndexMessage::Delete(sequences) => {
				self.delete_index_sequences(&sequences).await?;

				Completion::Index(sequences)
			},
			IndexMessage::Process(batch) => {
				self.process_index_batch_with_retry(&batch).await?;
				self.delete_index_sequences(&batch.sequences).await?;

				Completion::Index(batch.sequences)
			},
		};

		Ok(completion)
	}

	async fn process_archive_entry_with_retry(
		&self,
		entry: &crate::cache::archive::queue::Entry,
	) -> tg::Result<()> {
		tangram_futures::retry(&self.retry_options(), || async {
			match self.process_archive_entry(entry).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), sequence = entry.sequence, "failed to process an archive queue entry");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn process_index_batch_with_retry(&self, batch: &IndexBatch) -> tg::Result<()> {
		let result = tangram_futures::retry(&self.retry_options(), || async {
			match self.process_index_batch(&batch.fragments).await {
				Ok(result) => Ok(ControlFlow::Break(result)),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to process an index queue batch");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		if let Err(error) = result {
			tracing::error!(error = %error.trace(), "discarding a rejected index queue batch");
		}

		Ok(())
	}

	#[tracing::instrument(name = "archive.process_entry", level = "debug", skip_all, fields(indexer = %self.id(), object = %entry.object, sequence = entry.sequence), err(level = "debug"))]
	async fn process_archive_entry(
		&self,
		entry: &crate::cache::archive::queue::Entry,
	) -> tg::Result<()> {
		let object = self
			.try_wait_for_object_put(
				&self.server.config.object.archive_queue.retry,
				&entry.object,
				entry.put,
			)
			.await?
			.and_then(|object| object.bytes);
		let Some(bytes) = object else {
			tracing::error!(object = %entry.object, put = ?entry.put, "discarding an archive queue entry because the object put is absent from the cache");
			return Ok(());
		};
		let arg = tangram_archive::object::put::Arg {
			bytes: bytes.into_owned().into(),
			id: entry.object.clone(),
			put: entry.put,
		};
		self.server.archive_object(arg).await?;

		Ok(())
	}

	#[tracing::instrument(name = "index.process_batch", level = "debug", skip_all, fields(indexer = %self.id(), fragments = fragments.len(), bytes = fragments.iter().map(|fragment| fragment.payload.len()).sum::<usize>()), err(level = "debug"))]
	async fn process_index_batch(
		&self,
		fragments: &[crate::cache::index::queue::Fragment],
	) -> tg::Result<tg::Result<()>> {
		// Reassemble the encoded batch in fragment order before decoding it.
		let len = fragments
			.iter()
			.map(|fragment| fragment.payload.len())
			.sum();
		tracing::debug!(
			bytes = len,
			fragments = fragments.len(),
			"reassembling the index batch"
		);
		let mut bytes = Vec::with_capacity(len);
		for fragment in fragments {
			bytes.extend_from_slice(&fragment.payload);
		}
		tracing::debug!(bytes = bytes.len(), "decoding the index batch");
		let arg = tangram_index::batch::Arg::deserialize(&bytes)?;
		tracing::debug!(items = arg.items.len(), "decoded the index batch");
		drop(bytes);

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
		crate::checkpoint!(self.server, "index.batch").await;
		let result = self.server.index_batch_inner(arg).await?;

		Ok(result)
	}

	async fn delete_archive_sequence(&self, sequence: u64) -> tg::Result<()> {
		tangram_futures::retry(&self.retry_options(), || async {
			let arg = crate::cache::archive::queue::delete::Arg {
				indexer: self.id().clone(),
				sequence,
			};
			match self.server.cache.delete_archive_queue_entry(arg).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), %sequence, "failed to delete an archive queue entry");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn delete_index_sequences(&self, sequences: &[u64]) -> tg::Result<()> {
		for &sequence in sequences {
			tangram_futures::retry(&self.retry_options(), || async {
				let arg = crate::cache::index::queue::delete::Arg {
					indexer: self.id().clone(),
					sequence,
				};
				match self
					.server
					.cache
					.delete_index_queue_fragment(arg)
					.await
				{
					Ok(()) => Ok(ControlFlow::Break(())),
					Err(error) => {
						tracing::error!(error = %error.trace(), %sequence, "failed to delete an index queue fragment");

						Ok(ControlFlow::Continue(error))
					},
				}
			})
			.await?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {
		super::{IndexMessage, Queue, Queues},
		itertools::Itertools as _,
		std::collections::BTreeSet,
		tangram_client::prelude::*,
	};

	#[test]
	fn expiration_index_tracks_completion_and_abandonment() {
		let mut queues = Queues::empty();
		let id = tg::indexer::Id::new();
		let batch = crate::cache::index::queue::batch::Id::new([0; 16]);
		let fragment = crate::cache::index::queue::Fragment {
			batch,
			fragment: 0,
			fragments: 2,
			indexer: id,
			payload: bytes::Bytes::new(),
			sequence: 0,
		};
		let timeout = std::time::Duration::from_secs(60);
		queues.insert_index_fragment(fragment.clone(), None, timeout);
		assert_eq!(queues.batches.by_expires_at.len(), 1);
		assert_eq!(
			queues.next_batch_deadline(),
			Some(queues.batches.active[&batch].deadline)
		);
		let mut last = fragment.clone();
		last.fragment = 1;
		last.sequence = 1;
		queues.insert_index_fragment(last, None, timeout);
		assert!(queues.batches.active.is_empty());
		assert_eq!(queues.batches.complete.len(), 1);
		assert_eq!(queues.batches.by_expires_at.len(), 1);
		let expired = queues.expire_index_batches(timeout);
		assert!(expired.messages.is_empty());

		let mut fragment = fragment;
		fragment.batch = crate::cache::index::queue::batch::Id::new([1; 16]);
		queues.insert_index_fragment(fragment, None, timeout);
		assert_eq!(queues.batches.by_expires_at.len(), 2);
		queues.abandon_incomplete_batches();
		assert_eq!(queues.batches.by_expires_at.len(), 1);
	}

	#[test]
	fn expires_only_due_batches() {
		let mut queues = Queues::empty();
		let fragment = crate::cache::index::queue::Fragment {
			batch: crate::cache::index::queue::batch::Id::new([0; 16]),
			fragment: 0,
			fragments: 2,
			indexer: tg::indexer::Id::new(),
			payload: bytes::Bytes::new(),
			sequence: 0,
		};
		queues.insert_index_fragment(
			fragment,
			Some(tokio::sync::oneshot::channel().0),
			std::time::Duration::ZERO,
		);
		let output = queues.expire_index_batches(std::time::Duration::from_secs(60));
		assert_eq!(output.messages.len(), 1);
		assert!(output.responses[0].1.is_err());
		assert!(queues.batches.active.is_empty());
		assert_eq!(queues.batches.timed_out.len(), 1);
		assert_eq!(queues.batches.by_expires_at.len(), 1);
	}

	#[test]
	fn duplicate_fragments_preserve_complete_and_timed_out_responses() {
		for timed_out in [false, true] {
			let mut queues = Queues::empty();
			let fragment = crate::cache::index::queue::Fragment {
				batch: crate::cache::index::queue::batch::Id::new([0; 16]),
				fragment: 0,
				fragments: if timed_out { 2 } else { 1 },
				indexer: tg::indexer::Id::new(),
				payload: bytes::Bytes::new(),
				sequence: 0,
			};
			let timeout = std::time::Duration::ZERO;
			queues.insert_index_fragment(fragment.clone(), None, timeout);
			if timed_out {
				queues.expire_index_batches(std::time::Duration::from_secs(60));
			}
			let mut fragment = fragment;
			fragment.sequence = 1;
			let output = queues.insert_index_fragment(
				fragment,
				Some(tokio::sync::oneshot::channel().0),
				timeout,
			);
			assert!(
				matches!(output.messages.as_slice(), [IndexMessage::Delete(sequences)] if sequences == &[1])
			);
			assert_eq!(output.responses.len(), 1);
			assert_eq!(output.responses[0].1.is_err(), timed_out);
			let (_, batch) = queues.batches.by_expires_at.pop_first().unwrap();
			queues
				.batches
				.by_expires_at
				.insert((tokio::time::Instant::now(), batch));
			queues.expire_index_batches(timeout);
			assert!(queues.batches.complete.is_empty());
			assert!(queues.batches.timed_out.is_empty());
			assert!(queues.batches.by_expires_at.is_empty());
		}
	}

	#[test]
	fn advances_the_read_sequence_only_after_contiguous_completions() {
		let mut queue = Queue::new(2, 5);
		queue.complete(4);
		assert_eq!(queue.read_sequence, 2);
		queue.complete(2);
		assert_eq!(queue.read_sequence, 3);
		queue.complete(3);
		assert_eq!(queue.read_sequence, 5);
	}

	#[test]
	fn coalesces_a_million_completions_behind_a_stalled_entry() {
		let end = 1_000_001;
		let mut queue = Queue::new(0, end);
		for sequence in 1..end {
			queue.complete(sequence);
		}
		assert_eq!(queue.read_sequence, 0);
		assert_eq!(queue.completed.len(), 1);
		assert_eq!(queue.completed.get(&1), Some(&end));
		queue.complete(0);
		assert_eq!(queue.read_sequence, end);
		assert!(queue.completed.is_empty());
	}

	#[test]
	fn completion_ranges_match_individual_completions_in_every_order() {
		for sequences in (10..17).permutations(7) {
			let mut queue = Queue::new(10, 17);
			let mut completed = BTreeSet::new();
			for sequence in sequences {
				completed.insert(sequence);
				for sequence in [sequence, sequence, 9] {
					queue.complete(sequence);
				}
				let read_sequence = (10..17)
					.find(|sequence| !completed.contains(sequence))
					.unwrap_or(17);
				assert_eq!(queue.read_sequence, read_sequence);
				let expected = completed
					.range(read_sequence..)
					.copied()
					.collect::<BTreeSet<_>>();
				let actual = queue
					.completed
					.iter()
					.flat_map(|(&start, &end)| start..end)
					.collect::<BTreeSet<_>>();
				assert_eq!(actual, expected);
				assert!(
					queue
						.completed
						.iter()
						.all(|(&start, &end)| start > read_sequence && start < end)
				);
				assert!(
					queue
						.completed
						.iter()
						.tuple_windows()
						.all(|((_, end), (start, _))| end < start)
				);
			}
		}
	}

	#[test]
	fn completes_the_last_sequences_that_can_be_reserved() {
		let end = u64::try_from(i64::MAX).unwrap();
		let mut queue = Queue::new(end - 3, end);
		queue.complete(end - 1);
		queue.complete(end - 2);
		assert_eq!(queue.read_sequence, end - 3);
		queue.complete(end - 3);
		assert_eq!(queue.read_sequence, end);
		assert!(queue.completed.is_empty());
	}

	#[test]
	fn assembles_index_batches_in_fragment_order() {
		let indexer = tg::indexer::Id::new();
		let state = tangram_index::indexer::Indexer::new(indexer.clone());
		let mut queues = Queues::new(&state);
		let batch = crate::cache::index::queue::batch::Id::new([0; 16]);
		let fragment = crate::cache::index::queue::Fragment {
			batch,
			fragment: 1,
			fragments: 2,
			indexer: indexer.clone(),
			payload: bytes::Bytes::from_static(b"one"),
			sequence: 0,
		};
		let timeout = std::time::Duration::from_secs(1);
		let (sender, mut first) = tokio::sync::oneshot::channel();
		let output = queues.insert_index_fragment(fragment, Some(sender), timeout);
		assert!(matches!(
			first.try_recv(),
			Err(tokio::sync::oneshot::error::TryRecvError::Empty)
		));
		assert!(output.responses.is_empty());
		assert!(output.messages.is_empty());
		let fragment = crate::cache::index::queue::Fragment {
			batch,
			fragment: 0,
			fragments: 2,
			indexer,
			payload: bytes::Bytes::from_static(b"zero"),
			sequence: 1,
		};
		let (sender, mut second) = tokio::sync::oneshot::channel();
		let output = queues.insert_index_fragment(fragment, Some(sender), timeout);
		assert_eq!(output.responses.len(), 2);
		let [IndexMessage::Process(batch)] = output.messages.as_slice() else {
			panic!("expected one index batch");
		};
		assert_eq!(batch.fragments[0].fragment, 0);
		assert_eq!(batch.fragments[1].fragment, 1);
		for (sender, result) in output.responses {
			sender.send(result).unwrap();
		}
		assert!(first.try_recv().unwrap().is_ok());
		assert!(second.try_recv().unwrap().is_ok());
	}
}
