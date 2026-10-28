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
	pub arg: request::EnqueueRequestArg,
	pub sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

struct Assigned {
	entries: Vec<(Kind, u64, bytes::Bytes)>,
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
			.then(|request| Self::assign_sequences(state, changed, request))
			.for_each_concurrent(concurrency, |assigned| {
				self.write_entries(state, changed, assigned)
			})
			.await;

		Ok(())
	}

	async fn assign_sequences(
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		request: Request,
	) -> Assigned {
		let Request { arg, sender } = request;
		let request::EnqueueRequestArg { archive, index } = arg;
		let mut entries = Vec::with_capacity(2);
		let payloads = [(Kind::Archive, archive), (Kind::Index, index)];
		for (kind, payload) in payloads {
			let Some(payload) = payload else {
				continue;
			};

			// Assign the sequence and record the entry, so that it can be processed before its write finishes.
			let sequence = Self::allocate_sequence(state, changed, kind).await;
			state
				.lock()
				.unwrap()
				.queues
				.insert_entry(kind, sequence, payload.clone());
			entries.push((kind, sequence, payload));
		}
		changed.notify_waiters();

		Assigned { entries, sender }
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

	async fn write_entries(
		&self,
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		assigned: Assigned,
	) {
		let Assigned { entries, sender } = assigned;

		// Write each entry as its own queue item.
		let results = future::join_all(
			entries
				.iter()
				.map(|(kind, sequence, payload)| self.put_queue_item(*kind, *sequence, payload)),
		)
		.await;

		// Finish the writes.
		let mut state = state.lock().unwrap();
		for ((kind, sequence, _), result) in entries.iter().zip(&results) {
			state.queues.finish_write(*kind, *sequence, result.is_ok());
		}
		drop(state);
		changed.notify_waiters();

		// Respond.
		let result = results.into_iter().collect::<tg::Result<()>>();
		sender.send(result).ok();
	}
}
