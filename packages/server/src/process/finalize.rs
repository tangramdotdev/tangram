use {
	crate::{Server, database::Database},
	futures::{FutureExt as _, StreamExt as _, stream},
	indoc::{formatdoc, indoc},
	std::{collections::BTreeSet, ops::ControlFlow, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Stopper},
	tangram_index::prelude::*,
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
	pub(crate) process: tg::process::Id,
}

impl Server {
	pub(crate) async fn finalizer_task(
		&self,
		config: &crate::config::Finalizer,
		stopper: Stopper,
	) -> tg::Result<()> {
		let batch_size = config.message_batch_size.max(1);
		let subject = "processes.finalize.queue";
		let wakeups = self
			.messenger
			.subscribe_with_delivery::<()>(subject.into(), Delivery::One)
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
				let entries = match self.try_finalizer_dequeue_batch(batch_size).await {
					Ok(Some(entries)) => entries,
					Ok(None) => break,
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to dequeue finalize entries");
						tokio::time::sleep(Duration::from_secs(1)).await;
						break;
					},
				};
				let result = self.finalizer_handle_entries(entries).await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to handle finalize entries");
					tokio::time::sleep(Duration::from_secs(1)).await;
					break;
				}
				self.messenger
					.publish("processes.finalizer.progress".to_owned(), ())
					.await
					.ok();
			}
			if wakeups.next().await.is_none() {
				break;
			}
		}
		Ok(())
	}

	async fn try_finalizer_dequeue_batch(
		&self,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		match &self.process_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(process_store) => {
				self.try_finalizer_dequeue_batch_postgres(process_store, batch_size)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(process_store) => {
				self.try_finalizer_dequeue_batch_sqlite(process_store, batch_size)
					.await
			},
			#[cfg(feature = "turso")]
			Database::Turso(process_store) => {
				self.try_finalizer_dequeue_batch_turso(process_store, batch_size)
					.await
			},
		}
	}

	async fn finalizer_handle_entries(&self, entries: Vec<Entry>) -> tg::Result<()> {
		for entry in &entries {
			let result = self.finalizer_handle_entry(entry).await;
			if let Err(error) = result {
				return Err(tg::error!(
					!error,
					process = %entry.process,
					"failed to handle the process finalize entry"
				));
			}
		}
		Ok(())
	}

	async fn finalizer_handle_entry(&self, entry: &Entry) -> tg::Result<()> {
		let process = &entry.process;
		let session = self.session(&self.context);
		session
			.compact_process_log(process)
			.await
			.inspect_err(
				|error| tracing::error!(error = %error.trace(), %process, "failed to compact log"),
			)
			.ok();
		self.finalizer_spawn_index_task(process).await?;
		self.complete_process_finalize_entry(entry).await?;
		Ok(())
	}

	async fn complete_process_finalize_entry(&self, entry: &Entry) -> tg::Result<()> {
		let entry = entry.clone();
		self.process_store
			.run(|transaction| {
				let entry = entry.clone();
				async move {
					Self::complete_process_finalize_entry_with_transaction(transaction, &entry)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to complete the process finalize entry"))
	}

	async fn complete_process_finalize_entry_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		entry: &Entry,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update process_finalize_queue
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
		if n == 0 {
			return Ok(ControlFlow::Break(()));
		}
		let statement = formatdoc!(
			"
				delete from process_tokens
				where process = {p}1;
			"
		);
		let params = db::params![entry.process.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		let statement = formatdoc!(
			"
				delete from process_finalize_queue
				where position = {p}1;
			"
		);
		let params = db::params![entry.position];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n != 1 {
			return Err(tg::error!(
				"failed to delete the process finalize queue entry"
			));
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn try_get_process_finalize_queue_max_position(
		&self,
	) -> tg::Result<Option<i64>> {
		let connection = self
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let statement = indoc!(
			"
				select position
				from process_finalize_queue
				order by position desc
				limit 1;
			"
		);
		let params = db::params![];
		let position = connection
			.query_optional_value_into::<i64>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		drop(connection);
		Ok(position)
	}

	pub(crate) async fn get_process_finalize_queue_count_until_position(
		&self,
		position: i64,
	) -> tg::Result<u64> {
		let connection = self
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*)
				from process_finalize_queue
				where position <= {p}1;
			"
		);
		let params = db::params![position];
		let count = connection
			.query_one_value_into::<i64>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		drop(connection);
		Ok(u64::try_from(count).unwrap())
	}

	async fn finalizer_spawn_index_task(&self, id: &tg::process::Id) -> tg::Result<()> {
		let tg::process::get::Output { data, .. } = self
			.try_get_process_local(id, false)
			.await?
			.ok_or_else(|| tg::error!("failed to find the process"))?;
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let error = data.error.as_ref().map(|error| match error {
			tg::Either::Left(data) => {
				let mut children = BTreeSet::new();
				data.children(&mut children);
				children.into_iter().collect::<Vec<_>>()
			},
			tg::Either::Right(id) => {
				let id = id.clone().map_right(|id| id.id).into_inner().into();
				vec![id]
			},
		});
		let mut output = BTreeSet::new();
		if let Some(data) = &data.output {
			data.children(&mut output);
		}
		let output = data
			.output
			.as_ref()
			.map(|_| output.into_iter().collect::<Vec<_>>());
		let children = data
			.children
			.as_ref()
			.ok_or_else(|| tg::error!("expected the children to be set"))?
			.iter()
			.map(|child| {
				child
					.process
					.clone()
					.map_right(|process| process.id)
					.into_inner()
			})
			.collect();
		let put_process_arg = tangram_index::process::put::Arg {
			children: Some(children),
			command: data.command.clone().into(),
			error: Some(error),
			id: id.clone(),
			log: Some(
				data.log
					.clone()
					.map(|log| log.map_right(|log| log.id).into_inner().into()),
			),
			metadata: tg::process::Metadata::default(),
			output: Some(output),
			parent: None,
			stored: tangram_index::process::Stored::default(),
			touched_at: now,
		};
		self.index_tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					if let Err(error) = server
						.index
						.batch(tangram_index::batch::Arg {
							put_processes: vec![put_process_arg],
							..Default::default()
						})
						.await
					{
						tracing::error!(error = %error.trace(), "failed to put process to index");
					}
				}
			})
			.detach();
		Ok(())
	}

	pub(crate) fn spawn_publish_process_finalize_message_task(&self) {
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("processes.finalize.queue".into(), ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to publish the process finalize message");
					})
					.ok();
			}
		});
	}
}
