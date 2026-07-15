use {
	crate::{Server, database::Database},
	futures::{FutureExt as _, StreamExt as _, stream},
	indoc::formatdoc,
	std::{ops::ControlFlow, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Stopper},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

#[derive(Clone, Debug)]
pub(crate) struct Entry {
	pub(crate) position: i64,
	pub(crate) sandbox: tg::sandbox::Id,
}

impl Server {
	pub(crate) async fn sandbox_finalizer_task(
		&self,
		config: &crate::config::Finalizer,
		stopper: Stopper,
	) -> tg::Result<()> {
		let batch_size = config.message_batch_size.max(1);
		let subject = "sandboxes.finalize.queue".to_owned();
		let wakeups = self
			.messenger
			.queue_subscribe::<()>(subject.clone(), subject)
			.await
			.map_err(|error| tg::error!(!error, "failed to subscribe"))?
			.map(|_| ());
		let interval = config.message_batch_timeout.max(Duration::from_millis(1));
		let interval = IntervalStream::new(tokio::time::interval(interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(wakeups, interval).with_stopper(Some(stopper));
		let mut wakeups = pin!(wakeups);
		loop {
			loop {
				let entries = match self.try_dequeue_sandbox_finalize_batch(batch_size).await {
					Ok(Some(entries)) => entries,
					Ok(None) => break,
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to dequeue finalize entries");
						tokio::time::sleep(Duration::from_secs(1)).await;
						break;
					},
				};
				let result = self.handle_sandbox_finalize_entries(entries).await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to handle finalize entries");
					tokio::time::sleep(Duration::from_secs(1)).await;
					break;
				}
				self.messenger
					.publish("sandboxes.finalizer.progress".to_owned(), ())
					.await
					.ok();
			}
			if wakeups.next().await.is_none() {
				break;
			}
		}
		Ok(())
	}

	async fn try_dequeue_sandbox_finalize_batch(
		&self,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		match &self.process_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(process_store) => {
				self.try_dequeue_sandbox_finalize_batch_postgres(process_store, batch_size)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(process_store) => {
				self.try_dequeue_sandbox_finalize_batch_sqlite(process_store, batch_size)
					.await
			},
			#[cfg(feature = "turso")]
			Database::Turso(process_store) => {
				self.try_dequeue_sandbox_finalize_batch_turso(process_store, batch_size)
					.await
			},
		}
	}

	async fn handle_sandbox_finalize_entries(&self, entries: Vec<Entry>) -> tg::Result<()> {
		for entry in &entries {
			let result = self.handle_sandbox_finalize_entry(entry).await;
			if let Err(error) = result {
				return Err(tg::error!(
					!error,
					sandbox = %entry.sandbox,
					"failed to handle the sandbox finalize entry"
				));
			}
		}
		Ok(())
	}

	async fn handle_sandbox_finalize_entry(&self, entry: &Entry) -> tg::Result<()> {
		let entry = entry.clone();
		let server = self.clone();
		self.process_store
			.run(|transaction| {
				let entry = entry.clone();
				let server = server.clone();
				async move {
					server
						.handle_sandbox_finalize_entry_with_transaction(transaction, &entry)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to handle the sandbox finalize entry"))
	}

	async fn handle_sandbox_finalize_entry_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		entry: &Entry,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();

		let statement = formatdoc!(
			"
				update sandbox_finalize_queue
				set
					finished_at = {p}1,
					status = {p}2
				where position = {p}3;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, "finished", entry.position];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n != 1 {
			return Err(tg::error!(
				"failed to update the sandbox finalize queue entry"
			));
		}

		let statement = formatdoc!(
			"
				delete from sandbox_finalize_queue
				where position = {p}1;
			"
		);
		let params = db::params![entry.position];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n != 1 {
			return Err(tg::error!(
				"failed to delete the sandbox finalize queue entry"
			));
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn spawn_publish_sandbox_finalize_message_task(&self) {
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("sandboxes.finalize.queue".into(), ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to publish the sandbox finalize message");
					})
					.ok();
			}
		});
	}
}
