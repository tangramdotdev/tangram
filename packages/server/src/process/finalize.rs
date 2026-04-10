use {
	crate::{Server, database::Database},
	futures::{StreamExt as _, stream},
	indoc::{formatdoc, indoc},
	std::{collections::BTreeSet, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_index::prelude::*,
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[derive(Clone, Debug)]
pub(crate) struct Entry {
	pub(crate) position: i64,
	pub(crate) process: tg::process::Id,
}

impl Server {
	pub(crate) async fn finalizer_task(&self, config: &crate::config::Finalizer) -> tg::Result<()> {
		let batch_size = config.message_batch_size.max(1);
		let subject = "processes.finalize.queue";
		let group = "processes.finalize";
		let wakeup = self
			.messenger
			.subscribe::<()>(subject.into(), Some(group.into()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let interval = config.message_batch_timeout.max(Duration::from_millis(1));
		let interval = IntervalStream::new(tokio::time::interval(interval)).map(|_| ());
		let stream = stream::select(wakeup, interval);
		let mut stream = pin!(stream);
		while let Some(()) = stream.next().await {
			loop {
				let entries = match self.finalizer_try_dequeue_batch(batch_size).await {
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
					.publish("finalizer_progress".to_owned(), ())
					.await
					.ok();
			}
		}
		Ok(())
	}

	async fn finalizer_try_dequeue_batch(
		&self,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		match &self.sandbox_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(sandbox_store) => {
				self.finalizer_try_dequeue_batch_postgres(sandbox_store, batch_size)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(sandbox_store) => {
				self.finalizer_try_dequeue_batch_sqlite(sandbox_store, batch_size)
					.await
			},
		}
	}

	async fn finalizer_handle_entries(&self, entries: Vec<Entry>) -> tg::Result<()> {
		for (index, entry) in entries.iter().enumerate() {
			let result = self.finalizer_handle_entry(&entry.process).await;
			if let Err(error) = result {
				self.requeue_process_finalize_entries(&entries[index..])
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to requeue the process finalize entries")
					})?;
				return Err(tg::error!(
					!error,
					process = %entry.process,
					"failed to handle the process finalize entry"
				));
			}
		}
		Ok(())
	}

	async fn finalizer_handle_entry(&self, process: &tg::process::Id) -> tg::Result<()> {
		self.compact_process_log(process)
			.await
			.inspect_err(
				|error| tracing::error!(error = %error.trace(), %process, "failed to compact log"),
			)
			.ok();
		self.finalizer_spawn_index_task(process).await?;
		Ok(())
	}

	async fn requeue_process_finalize_entries(&self, entries: &[Entry]) -> tg::Result<()> {
		if entries.is_empty() {
			return Ok(());
		}
		let mut connection = self
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to acquire a transaction"))?;
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into process_finalize_queue (position, process)
				values ({p}1, {p}2);
			"
		);
		for entry in entries {
			let params = db::params![entry.position, entry.process.to_string()];
			transaction
				.execute(statement.clone().into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(crate) async fn try_get_process_finalize_queue_max_position(
		&self,
	) -> tg::Result<Option<i64>> {
		let connection =
			self.sandbox_store.connection().await.map_err(|source| {
				tg::error!(!source, "failed to get a sandbox store connection")
			})?;
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
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		Ok(position)
	}

	pub(crate) async fn get_process_finalize_queue_count_until_position(
		&self,
		position: i64,
	) -> tg::Result<u64> {
		let connection =
			self.sandbox_store.connection().await.map_err(|source| {
				tg::error!(!source, "failed to get a sandbox store connection")
			})?;
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
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		Ok(u64::try_from(count).unwrap())
	}

	async fn finalizer_spawn_index_task(&self, id: &tg::process::Id) -> tg::Result<()> {
		let tg::process::get::Output { data, .. } = self
			.try_get_process_local(id, false)
			.await?
			.ok_or_else(|| tg::error!("failed to find the process"))?;
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let command = (
			data.command.clone().into(),
			tangram_index::ProcessObjectKind::Command,
		);
		let errors = data
			.error
			.as_ref()
			.into_iter()
			.flat_map(|error| match error {
				tg::Either::Left(data) => {
					let mut children = BTreeSet::new();
					data.children(&mut children);
					children
						.into_iter()
						.map(|object| {
							let kind = tangram_index::ProcessObjectKind::Error;
							(object, kind)
						})
						.collect::<Vec<_>>()
				},
				tg::Either::Right(id) => {
					let id = id.clone().into();
					let kind = tangram_index::ProcessObjectKind::Error;
					vec![(id, kind)]
				},
			});
		let log = data.log.as_ref().map(|id| {
			let id = id.clone().into();
			let kind = tangram_index::ProcessObjectKind::Log;
			(id, kind)
		});
		let mut outputs = BTreeSet::new();
		if let Some(output) = &data.output {
			output.children(&mut outputs);
		}
		let outputs = outputs.into_iter().map(|object| {
			let kind = tangram_index::ProcessObjectKind::Output;
			(object, kind)
		});
		let objects = std::iter::once(command)
			.chain(errors)
			.chain(log)
			.chain(outputs)
			.collect();
		let children = data
			.children
			.as_ref()
			.ok_or_else(|| tg::error!("expected the children to be set"))?
			.iter()
			.map(|child| child.process.clone())
			.collect();
		let put_process_arg = tangram_index::PutProcessArg {
			children,
			stored: tangram_index::ProcessStored::default(),
			id: id.clone(),
			metadata: tg::process::Metadata::default(),
			objects,
			touched_at: now,
		};
		self.index_tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					if let Err(error) = server
						.index
						.put(tangram_index::PutArg {
							processes: vec![put_process_arg],
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
