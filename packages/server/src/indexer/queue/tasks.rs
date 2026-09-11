use {
	crate::indexer::{Indexer, RETRY_OPTIONS, State, queue},
	futures::future,
	std::{ops::ControlFlow, sync::Mutex},
	tangram_client::prelude::*,
};
impl Indexer {
	pub(in crate::indexer) async fn sequence_reservations_task(
		&self,
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		kind: queue::Kind,
	) -> tg::Result<()> {
		loop {
			let notified = changed.notified();
			let reservation = {
				let mut state = state.lock().unwrap();
				if state.available || state.writes > 0 {
					state.queues.start_reservation(self, kind)?
				} else {
					None
				}
			};
			if let Some(reservation) = reservation {
				self.persist_reservation_with_retry(reservation).await?;
				state.lock().unwrap().queues.finish_reservation(reservation);
				changed.notify_waiters();
			} else {
				notified.await;
			}
		}
	}

	pub(in crate::indexer) async fn queue_completions_task(
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		mut receiver: queue::CompletionReceiver,
	) -> tg::Result<()> {
		loop {
			let completion = receiver
				.recv()
				.await
				.ok_or_else(|| tg::error!("an indexer queue task stopped"))?;
			state.lock().unwrap().queues.complete(completion);
			changed.notify_waiters();
		}
	}

	pub(in crate::indexer) async fn batch_expiration_task(
		&self,
		index_sender: &queue::IndexMessageSender,
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
	) -> tg::Result<()> {
		loop {
			let notified = changed.notified();
			let deadline = state.lock().unwrap().queues.next_batch_deadline();
			let sleep = async {
				if let Some(deadline) = deadline {
					tokio::time::sleep_until(deadline).await;
				} else {
					future::pending::<()>().await;
				}
			};
			tokio::select! { () = notified => continue, () = sleep => {} }
			let output = state
				.lock()
				.unwrap()
				.queues
				.expire_index_batches(self.server.config.object.index_queue.batch_timeout);
			for message in output.messages {
				index_sender
					.send(message)
					.await
					.map_err(|_| tg::error!("the index queue task stopped"))?;
			}
			for (sender, result) in output.responses {
				sender.send(result).ok();
			}
		}
	}

	pub(in crate::indexer) async fn queue_checkpoints_task(
		&self,
		state: &Mutex<State>,
		mut checkpoints: tokio::sync::mpsc::UnboundedReceiver<
			tokio::sync::oneshot::Sender<tg::Result<()>>,
		>,
	) -> tg::Result<()> {
		let mut interval =
			tokio::time::interval(self.server.config.object.queue_checkpoint_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut periodic = true;
		loop {
			let sender = tokio::select! {
				sender = checkpoints.recv() => {
					periodic = false;
					Some(sender.ok_or_else(|| tg::error!("the queue checkpoint request channel closed"))?)
				},
				_ = interval.tick(), if periodic => None,
			};
			let result = self.checkpoint_queues(state).await;
			if let Some(sender) = sender {
				sender.send(result).ok();
			} else if let Err(error) = result {
				tracing::error!(error = %error.trace(), "failed to checkpoint the queues");
			}
		}
	}

	pub(in crate::indexer) async fn checkpoint_queues(
		&self,
		state: &Mutex<State>,
	) -> tg::Result<()> {
		let (archive, index) = state.lock().unwrap().queues.read_sequences();
		self.checkpoint_read_sequences(archive, index).await?;
		Ok(())
	}

	pub(in crate::indexer) async fn drain_queues(
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		index_sender: &queue::IndexMessageSender,
	) -> tg::Result<()> {
		state.lock().unwrap().available = false;
		loop {
			let notified = changed.notified();
			let finished = {
				let state = state.lock().unwrap();
				state.writes == 0 && !state.queues.reservations_pending()
			};
			if finished {
				break;
			}
			notified.await;
		}
		let (target_sequences, output) = {
			let mut state = state.lock().unwrap();
			(
				state.queues.target_sequences(),
				state.queues.abandon_incomplete_batches(),
			)
		};
		for message in output.messages {
			index_sender
				.send(message)
				.await
				.map_err(|_| tg::error!("the index queue task stopped"))?;
		}
		for (sender, result) in output.responses {
			sender.send(result).ok();
		}
		loop {
			let notified = changed.notified();
			let finished = {
				let state = state.lock().unwrap();
				state.queues.drained(target_sequences.0, target_sequences.1)
					&& !state.limits.has_queue_requests()
			};
			if finished {
				break;
			}
			notified.await;
		}

		Ok(())
	}

	pub(in crate::indexer) async fn checkpoint_queues_with_retry(
		&self,
		checkpoints: &queue::CheckpointSender,
	) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			let (sender, receiver) = tokio::sync::oneshot::channel();
			let result = match checkpoints.send(sender) {
				Ok(()) => receiver
					.await
					.map_err(|_| tg::error!("the queue checkpoint task stopped"))?,
				Err(_) => Err(tg::error!("the queue checkpoint task stopped")),
			};
			match result {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to checkpoint the queues");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}
}
