use {
	super::{Indexer, RETRY_OPTIONS},
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
	},
	tangram_archive::Archive as _,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_index::prelude::*,
	tangram_store::Store as _,
};

pub(super) type CompletionReceiver = tokio::sync::mpsc::UnboundedReceiver<Completion>;
pub(super) type CompletionSender = tokio::sync::mpsc::UnboundedSender<Completion>;
pub(super) type MessageReceiver = tokio::sync::mpsc::UnboundedReceiver<Message>;
pub(super) type MessageSender = tokio::sync::mpsc::UnboundedSender<Message>;

pub(super) struct Actions {
	pub messages: Vec<Message>,
	pub responses: Vec<(String, tg::Result<()>)>,
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
pub(super) struct Reservation {
	end: u64,
	kind: Kind,
}

pub(super) struct Queues {
	archive: Queue,
	batches: Batches,
	index: Queue,
}

pub(super) enum Message {
	Archive(crate::store::object::archive::queue::Entry),
	DeleteArchive(u64),
	DeleteIndex(Vec<u64>),
	Index(IndexBatch),
}

struct Queue {
	completed: BTreeSet<u64>,
	read_sequence: u64,
	reservation_pending: bool,
	reserved_sequence_end: u64,
	write_sequence: u64,
}

#[derive(Default)]
struct Batches {
	active: BTreeMap<crate::store::object::index::queue::batch::Id, Batch>,
	retired: BTreeMap<crate::store::object::index::queue::batch::Id, RetiredBatch>,
}

struct Batch {
	deadline: tokio::time::Instant,
	fragments: BTreeMap<u64, BatchFragment>,
	len: u64,
	waiters: Vec<String>,
}

struct BatchFragment {
	fragment: crate::store::object::index::queue::Fragment,
	sequences: Vec<u64>,
}

pub(super) struct IndexBatch {
	fragments: Vec<crate::store::object::index::queue::Fragment>,
	sequences: Vec<u64>,
}

struct RetiredBatch {
	expires_at: tokio::time::Instant,
	status: RetiredBatchStatus,
}

#[derive(Clone, Copy)]
enum RetiredBatchStatus {
	Complete,
	TimedOut,
}

impl Actions {
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
	pub fn new(indexer: &crate::store::indexer::Indexer) -> Self {
		let archive = Queue::new(
			indexer.archive_read_sequence,
			indexer.archive_write_sequence,
		);
		let index = Queue::new(indexer.index_read_sequence, indexer.index_write_sequence);
		Self {
			archive,
			batches: Batches::default(),
			index,
		}
	}

	pub async fn recover(&mut self, indexer: &Indexer, sender: &MessageSender) -> tg::Result<()> {
		for sequence in self.archive.read_sequence..self.archive.reserved_sequence_end {
			let arg = crate::store::object::archive::queue::get::Arg {
				indexer: indexer.id.clone(),
				sequence,
			};
			let entry = indexer
				.server
				.store
				.try_get_object_archive_queue_entry(arg)
				.await
				.map_err(
					|error| tg::error!(!error, %sequence, "failed to recover an archive queue entry"),
				)?;
			if let Some(entry) = entry {
				sender
					.send(Message::Archive(entry))
					.map_err(|_| tg::error!("the object queue task stopped"))?;
			} else {
				self.archive.complete(sequence);
			}
		}

		for sequence in self.index.read_sequence..self.index.reserved_sequence_end {
			let arg = crate::store::object::index::queue::get::Arg {
				indexer: indexer.id.clone(),
				sequence,
			};
			let fragment = indexer
				.server
				.store
				.try_get_object_index_queue_fragment(arg)
				.await
				.map_err(
					|error| tg::error!(!error, %sequence, "failed to recover an index queue fragment"),
				)?;
			if let Some(fragment) = fragment {
				let timeout = indexer.server.config.object.index_queue.batch_timeout;
				let actions = self.insert_index_fragment(fragment, None, timeout);
				for message in actions.messages {
					sender
						.send(message)
						.map_err(|_| tg::error!("the object queue task stopped"))?;
				}
			} else {
				self.index.complete(sequence);
			}
		}

		Ok(())
	}

	pub async fn reserve_initial_sequences(&mut self, indexer: &Indexer) -> tg::Result<()> {
		for kind in [Kind::Archive, Kind::Index] {
			let reservation = self
				.start_reservation(indexer, kind)?
				.ok_or_else(|| tg::error!("failed to prepare an object queue reservation"))?;
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
	) -> tg::Result<Option<Reservation>> {
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
			.ok_or_else(|| tg::error!("the object queue sequence was exhausted"))?;
		queue.reservation_pending = true;
		let reservation = Reservation { end, kind };

		Ok(Some(reservation))
	}

	pub fn finish_reservation(&mut self, reservation: Reservation) {
		let queue = self.queue_mut(reservation.kind);
		queue.reservation_pending = false;
		queue.reserved_sequence_end = reservation.end;
	}

	pub fn cancel_reservation(&mut self, reservation: Reservation) {
		self.queue_mut(reservation.kind).reservation_pending = false;
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

	#[must_use]
	pub fn index_read_sequence(&self) -> u64 {
		self.index.read_sequence
	}

	#[must_use]
	pub fn index_target(&self) -> u64 {
		self.index.write_sequence
	}

	pub fn insert_index_fragment(
		&mut self,
		fragment: crate::store::object::index::queue::Fragment,
		waiter: Option<String>,
		batch_timeout: std::time::Duration,
	) -> Actions {
		let mut actions = Actions::new();
		let batch_id = fragment.batch;
		if let Some(retired) = self.batches.retired.get(&batch_id) {
			actions
				.messages
				.push(Message::DeleteIndex(vec![fragment.sequence]));
			if let Some(waiter) = waiter {
				let result = match retired.status {
					RetiredBatchStatus::Complete => Ok(()),
					RetiredBatchStatus::TimedOut => {
						Err(tg::error!("the index queue batch timed out"))
					},
				};
				actions.responses.push((waiter, result));
			}

			return actions;
		}
		let batch = self
			.batches
			.active
			.entry(batch_id)
			.or_insert_with(|| Batch {
				deadline: tokio::time::Instant::now() + batch_timeout,
				fragments: BTreeMap::new(),
				len: fragment.fragments,
				waiters: Vec::new(),
			});
		if batch.len != fragment.fragments {
			actions
				.messages
				.push(Message::DeleteIndex(vec![fragment.sequence]));
			if let Some(waiter) = waiter {
				actions.responses.push((
					waiter,
					Err(tg::error!("conflicting index queue fragment counts")),
				));
			}

			return actions;
		}
		if let Some(existing) = batch.fragments.get_mut(&fragment.fragment) {
			if existing.fragment.payload != fragment.payload {
				actions
					.messages
					.push(Message::DeleteIndex(vec![fragment.sequence]));
				if let Some(waiter) = waiter {
					actions.responses.push((
						waiter,
						Err(tg::error!("conflicting index queue fragment payloads")),
					));
				}

				return actions;
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
			return actions;
		}
		let batch = self.batches.active.remove(&batch_id).unwrap();
		self.batches.retired.insert(
			batch_id,
			RetiredBatch {
				expires_at: tokio::time::Instant::now() + batch_timeout,
				status: RetiredBatchStatus::Complete,
			},
		);
		let mut fragments = Vec::with_capacity(batch.fragments.len());
		let mut sequences = Vec::new();
		for fragment in batch.fragments.into_values() {
			fragments.push(fragment.fragment);
			sequences.extend(fragment.sequences);
		}
		actions
			.responses
			.extend(batch.waiters.into_iter().map(|waiter| (waiter, Ok(()))));
		actions.messages.push(Message::Index(IndexBatch {
			fragments,
			sequences,
		}));

		actions
	}

	pub fn expire_index_batches(&mut self, batch_timeout: std::time::Duration) -> Actions {
		let mut actions = Actions::new();
		let now = tokio::time::Instant::now();
		self.batches
			.retired
			.retain(|_, batch| batch.expires_at > now);
		let ids = self
			.batches
			.active
			.iter()
			.filter_map(|(id, batch)| (batch.deadline <= now).then_some(*id))
			.collect::<Vec<_>>();
		for id in ids {
			let batch = self.batches.active.remove(&id).unwrap();
			self.batches.retired.insert(
				id,
				RetiredBatch {
					expires_at: now + batch_timeout,
					status: RetiredBatchStatus::TimedOut,
				},
			);
			actions.responses.extend(
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
			actions.messages.push(Message::DeleteIndex(sequences));
		}

		actions
	}

	#[must_use]
	pub fn next_batch_deadline(&self) -> Option<tokio::time::Instant> {
		self.batches
			.active
			.values()
			.map(|batch| batch.deadline)
			.chain(self.batches.retired.values().map(|batch| batch.expires_at))
			.min()
	}

	pub fn abandon_incomplete_batches(&mut self) -> Actions {
		let mut actions = Actions::new();
		let active = std::mem::take(&mut self.batches.active);
		for (_, batch) in active {
			actions.responses.extend(
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
			actions.messages.push(Message::DeleteIndex(sequences));
		}

		actions
	}

	#[must_use]
	pub fn drained(&self, archive_target: u64, index_target: u64) -> bool {
		self.archive.read_sequence >= archive_target && self.index.read_sequence >= index_target
	}

	pub async fn checkpoint(&self, indexer: &Indexer) -> tg::Result<()> {
		let (archive_read_sequence, index_read_sequence) = self.read_sequences();
		indexer
			.checkpoint_read_sequences(archive_read_sequence, index_read_sequence)
			.await
	}

	#[must_use]
	pub fn read_sequences(&self) -> (u64, u64) {
		(self.archive.read_sequence, self.index.read_sequence)
	}

	#[must_use]
	pub fn targets(&self) -> (u64, u64) {
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
			completed: BTreeSet::new(),
			read_sequence,
			reservation_pending: false,
			reserved_sequence_end: write_sequence,
			write_sequence,
		}
	}

	fn complete(&mut self, sequence: u64) {
		self.completed.insert(sequence);
		while self.completed.remove(&self.read_sequence) {
			self.read_sequence += 1;
		}
	}
}

impl Indexer {
	pub(super) async fn checkpoint_read_sequences(
		&self,
		archive_read_sequence: u64,
		index_read_sequence: u64,
	) -> tg::Result<()> {
		let arg = crate::store::indexer::update::Arg {
			id: self.id.clone(),
			value: crate::store::indexer::update::Value::ArchiveReadSequence(archive_read_sequence),
		};
		self.server.store.update_indexer(arg).await?;
		let arg = crate::store::indexer::update::Arg {
			id: self.id.clone(),
			value: crate::store::indexer::update::Value::IndexReadSequence(index_read_sequence),
		};
		self.server.store.update_indexer(arg).await?;

		Ok(())
	}

	pub(super) async fn persist_reservation_with_retry(
		&self,
		reservation: Reservation,
	) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			match self.persist_reservation(reservation).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to reserve object queue sequences");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn persist_reservation(&self, reservation: Reservation) -> tg::Result<()> {
		let value = match reservation.kind {
			Kind::Archive => {
				crate::store::indexer::update::Value::ArchiveWriteSequence(reservation.end)
			},
			Kind::Index => {
				crate::store::indexer::update::Value::IndexWriteSequence(reservation.end)
			},
		};
		let arg = crate::store::indexer::update::Arg {
			id: self.id.clone(),
			value,
		};
		self.server
			.store
			.update_indexer(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to reserve object queue sequences"))?;

		Ok(())
	}

	pub(super) async fn queue_task(
		&self,
		mut receiver: MessageReceiver,
		sender: CompletionSender,
		stopper: Stopper,
	) -> tg::Result<()> {
		let mut operations = FuturesUnordered::new();
		loop {
			if receiver.is_closed() && operations.is_empty() {
				break;
			}
			tokio::select! {
				() = stopper.wait() => break,
				message = receiver.recv(), if !receiver.is_closed() => {
					let Some(message) = message else {
						continue;
					};
					operations.push(self.process_queue_message(message).boxed());
				},
				completion = operations.next(), if !operations.is_empty() => {
					let completion = completion.unwrap()?;
					sender
						.send(completion)
						.map_err(|_| tg::error!("the indexer request task stopped"))?;
				},
			}
		}

		Ok(())
	}

	async fn process_queue_message(&self, message: Message) -> tg::Result<Completion> {
		let completion = match message {
			Message::Archive(entry) => {
				self.process_archive_entry_with_retry(&entry).await?;
				self.delete_archive_sequence(entry.sequence).await?;

				Completion::Archive(entry.sequence)
			},
			Message::DeleteArchive(sequence) => {
				self.delete_archive_sequence(sequence).await?;

				Completion::Archive(sequence)
			},
			Message::DeleteIndex(sequences) => {
				self.delete_index_sequences(&sequences).await?;

				Completion::Index(sequences)
			},
			Message::Index(batch) => {
				self.process_index_batch_with_retry(&batch).await?;
				self.delete_index_sequences(&batch.sequences).await?;

				Completion::Index(batch.sequences)
			},
		};

		Ok(completion)
	}

	async fn process_archive_entry_with_retry(
		&self,
		entry: &crate::store::object::archive::queue::Entry,
	) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
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
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			match self.process_index_batch(&batch.fragments).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to process an index queue batch");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn process_archive_entry(
		&self,
		entry: &crate::store::object::archive::queue::Entry,
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
			tracing::error!(object = %entry.object, put = ?entry.put, "discarding an archive queue entry because the object put is absent from the store");
			return Ok(());
		};
		let Some(archive) = &self.server.archive else {
			return Err(tg::error!("the archive is unavailable"));
		};
		let arg = tangram_archive::object::put::Arg {
			bytes: bytes.into_owned().into(),
			id: entry.object.clone(),
			put: entry.put,
		};
		archive.put_object(arg).await.map_err(
			|error| tg::error!(!error, id = %entry.object, "failed to put an object in the archive"),
		)?;
		if let Some(config) = &self.server.config.object.cache {
			let arg = crate::store::object::cache::put::Arg {
				cache: uuid::Uuid::now_v7().into_bytes(),
				id: entry.object.clone(),
				partition: rand::random_range(0..config.partition_total),
				put: entry.put,
			};
			self.server.store.put_object_cache_entry(arg).await?;
		}

		Ok(())
	}

	async fn process_index_batch(
		&self,
		fragments: &[crate::store::object::index::queue::Fragment],
	) -> tg::Result<()> {
		let args = fragments
			.iter()
			.map(|fragment| tangram_index::batch::Arg::deserialize(&fragment.payload))
			.collect::<tg::Result<Vec<_>>>()?;
		let puts = args
			.iter()
			.flat_map(|arg| &arg.items)
			.filter_map(|item| match item {
				tangram_index::batch::Item::PutObject(arg) => Some((arg.id.clone(), arg.put)),
				_ => None,
			})
			.collect::<BTreeSet<_>>();
		let results = future::try_join_all(puts.into_iter().map(|(id, put)| async move {
			let contains = self
				.wait_for_object_put(&self.server.config.object.index_queue.retry, &id, put)
				.await?;

			Ok::<_, tg::Error>((id, put, contains))
		}))
		.await?;
		let missing = results
			.into_iter()
			.filter(|(_, _, exists)| !exists)
			.collect::<Vec<_>>();
		if let Some((id, put, _)) = missing.first() {
			tracing::error!(%id, ?put, missing_count = missing.len(), "discarding an index queue batch because an object put is absent from the store");
			return Ok(());
		}
		for arg in args {
			crate::checkpoint!(self.server, "index.batch").await;
			self.server.index.batch(arg).await?;
		}

		Ok(())
	}

	async fn delete_archive_sequence(&self, sequence: u64) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			let arg = crate::store::object::archive::queue::delete::Arg {
				indexer: self.id.clone(),
				sequence,
			};
			match self
				.server
				.store
				.delete_object_archive_queue_entry(arg)
				.await
			{
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
			tangram_futures::retry(&RETRY_OPTIONS, || async {
				let arg = crate::store::object::index::queue::delete::Arg {
					indexer: self.id.clone(),
					sequence,
				};
				match self
					.server
					.store
					.delete_object_index_queue_fragment(arg)
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
		super::{Message, Queue, Queues},
		tangram_client::prelude::*,
	};

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
	fn assembles_index_batches_in_fragment_order() {
		let indexer = tg::indexer::Id::new();
		let state = crate::store::indexer::Indexer::new(indexer.clone());
		let mut queues = Queues::new(&state);
		let batch = crate::store::object::index::queue::batch::Id::new([0; 16]);
		let fragment = crate::store::object::index::queue::Fragment {
			batch,
			fragment: 1,
			fragments: 2,
			indexer: indexer.clone(),
			payload: bytes::Bytes::from_static(b"one"),
			sequence: 0,
		};
		let timeout = std::time::Duration::from_secs(1);
		let actions = queues.insert_index_fragment(fragment, Some("one".into()), timeout);
		assert!(actions.responses.is_empty());
		assert!(actions.messages.is_empty());
		let fragment = crate::store::object::index::queue::Fragment {
			batch,
			fragment: 0,
			fragments: 2,
			indexer,
			payload: bytes::Bytes::from_static(b"zero"),
			sequence: 1,
		};
		let actions = queues.insert_index_fragment(fragment, Some("zero".into()), timeout);
		assert_eq!(actions.responses.len(), 2);
		let [Message::Index(batch)] = actions.messages.as_slice() else {
			panic!("expected one index batch");
		};
		assert_eq!(batch.fragments[0].fragment, 0);
		assert_eq!(batch.fragments[1].fragment, 1);
	}
}
