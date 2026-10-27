use {
	super::Kind,
	crate::{Indexer, IndexerState, request},
	futures::{StreamExt as _, future},
	std::sync::Mutex,
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<Request>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<Request>;

pub(crate) struct Request {
	pub archive: Vec<request::Fragment>,
	pub index: Vec<request::Fragment>,
	pub sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

struct Assigned {
	error: Option<tg::Error>,
	fragments: Vec<(Kind, u64, request::Fragment)>,
	sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

impl Indexer {
	pub(crate) async fn write_task(
		&self,
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		receiver: Receiver,
	) -> tg::Result<()> {
		// Assign sequences in the order the requests arrive, and write the assigned requests concurrently, which stops reading the channel while the writes are at the concurrency.
		let concurrency = self.server.config.indexer.writer.concurrency;
		tokio_stream::wrappers::ReceiverStream::new(receiver)
			.then(|request| self.assign_sequences(state, changed, request))
			.for_each_concurrent(concurrency, |assigned| {
				self.write_fragments(state, changed, assigned)
			})
			.await;

		Ok(())
	}

	async fn assign_sequences(
		&self,
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		request: Request,
	) -> Assigned {
		let Request {
			archive,
			index,
			sender,
		} = request;
		let mut error = None;
		let mut fragments = Vec::with_capacity(archive.len() + index.len());
		let requests = archive
			.into_iter()
			.map(|fragment| (Kind::Archive, fragment))
			.chain(index.into_iter().map(|fragment| (Kind::Index, fragment)));
		for (kind, fragment) in requests {
			// Assign the sequence and record the fragment, so that its batch can be processed before the write finishes.
			let sequence = Self::allocate_sequence(state, changed, kind).await;
			let timeout = self.batch_timeout(kind);
			let mut state = state.lock().unwrap();
			let result = state
				.queues
				.insert_fragment(kind, sequence, &fragment, timeout);
			if let Err(source) = result {
				state.queues.finish_write(kind, sequence, &fragment, false);
				error.get_or_insert(source);
				continue;
			}
			drop(state);
			fragments.push((kind, sequence, fragment));
		}
		changed.notify_waiters();

		Assigned {
			error,
			fragments,
			sender,
		}
	}

	async fn allocate_sequence(
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		kind: Kind,
	) -> u64 {
		loop {
			let notified = changed.notified();
			let sequence = state.lock().unwrap().queues.try_allocate_sequence(kind);
			if let Some(sequence) = sequence {
				changed.notify_waiters();
				return sequence;
			}
			notified.await;
		}
	}

	async fn write_fragments(
		&self,
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		assigned: Assigned,
	) {
		let Assigned {
			mut error,
			fragments,
			sender,
		} = assigned;

		// Write each fragment as its own queue item.
		let results = future::join_all(
			fragments
				.iter()
				.map(|(kind, sequence, fragment)| self.put_queue_item(*kind, *sequence, fragment)),
		)
		.await;

		// Finish the writes.
		let mut state = state.lock().unwrap();
		for ((kind, sequence, fragment), result) in fragments.iter().zip(&results) {
			state
				.queues
				.finish_write(*kind, *sequence, fragment, result.is_ok());
		}
		drop(state);
		changed.notify_waiters();

		// Respond.
		for result in results {
			if let Err(source) = result {
				error.get_or_insert(source);
			}
		}
		let result = error.map_or(Ok(()), Err);
		sender.send(result).ok();
	}
}
