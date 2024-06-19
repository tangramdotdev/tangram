use crate::Server;
use bytes::Bytes;
use futures::{future, stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::{pin::pin, sync::Arc};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*, Error};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub(crate) async fn object_indexer_task(&self) -> tg::Result<()> {
		// Subscribe to object indexing events.
		let mut events = self
			.messenger
			.subscribe("objects.index".to_owned(), Some("queue".to_owned()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.boxed();

		let semaphore = Arc::new(tokio::sync::Semaphore::new(
			self.options.object_indexer.as_ref().unwrap().batch_size.to_usize().unwrap()
		));

		loop {
			// Sleep until we can do some work.
			if semaphore.available_permits() == 0 {
				tokio::time::sleep(std::time::Duration::from_millis(50)).await;
				continue;
			}

			// Attempt to get an object to index.
			let connection = match self.database.connection(db::Priority::Low).await {
				Ok(connection) => connection,
				Err(error) => {
					tracing::error!(?error, "failed to get a database connection");
					let duration = std::time::Duration::from_secs(1);
					tokio::time::sleep(duration).await;
					continue;
				},
			};

			#[derive(serde::Deserialize)]
			struct Row {
				id: tg::object::Id,
			}
			let p = connection.p();

			let statement = formatdoc!(
				"
					update objects
					set
						index_status = 'started',
						index_started_at = {p}1
					where id in (
						select id
						from objects
						where
							index_status = 'enqueued' or
							(index_status = 'started' and index_started_at <= {p}2)
						limit {p}3
					)
					returning id;
				"
			);
			let limit = semaphore.available_permits();
			let timeout = self.options.object_indexer.as_ref().unwrap().timeout;
			let now = (time::OffsetDateTime::now_utc()).format(&Rfc3339).unwrap();
			let time = (time::OffsetDateTime::now_utc()
				- std::time::Duration::from_secs_f64(timeout))
			.format(&Rfc3339)
			.unwrap();
			let params = db::params![now, time, limit];
			let result = connection
				.query_all_into::<Row>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"));
			drop(connection);
			let rows = match result {
				Ok(rows) if !rows.is_empty() => rows,

				// If there are no objects enqueued for indexing, then wait to receive an object indexing event or for a timeout to pass.
				Ok(_) => {
					let timeout = std::time::Duration::from_secs_f64(timeout);
					let timeout = tokio::time::sleep(timeout);
					future::select(events.next(), pin!(timeout)).await;
					continue;
				},

				Err(error) => {
					tracing::error!(?error, "failed to get an object to index");
					let duration = std::time::Duration::from_secs(1);
					tokio::time::sleep(duration).await;
					continue;
				},
			};

			// Spawn tasks to index objects in a batch.
			for row in rows {
				tokio::spawn({
					let semaphore = semaphore.clone();
					let server = self.clone();
					async move {
						let _permit = semaphore.acquire().await.unwrap();
						server
							.index_object(&row.id)
							.await
							.inspect_err(|error| tracing::error!(?error))
							.ok();
					}
				});
			}
		}
	}

	async fn index_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		// Get a short lived connection
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object.
		#[derive(serde::Deserialize)]
		struct Row {
			bytes: Option<Bytes>,
			children: bool,
			complete: bool,
			count: Option<u64>,
			weight: Option<u64>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select bytes, children, complete, count, weight
				from objects
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let Row {
			bytes,
			children,
			complete,
			count,
			weight,
		} = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(bytes) = bytes else {
			return Ok(());
		};
		drop(connection);

		// Deserialize the object.
		let data = tg::object::Data::deserialize(id.kind(), &bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;

		// Get the children's metadata.
		let children_metadata: Vec<Option<tg::object::Metadata>> = data
			.children()
			.into_iter()
			.map(|id| async move { self.try_get_object_metadata(&id).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Get a connection
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// If the children were not set, then add them.
		if !children {
			loop {
				let transaction = connection
					.transaction()
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
				let p = transaction.p();

				// Add the children.
				for child in data.children() {
					let statement = formatdoc!(
						"
							insert into object_children (object, child)
							values ({p}1, {p}2)
							on conflict (object, child) do nothing;
						"
					);
					let params = db::params![id, child];
					transaction
						.execute(statement, params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}

				// Set the children flag.
				let statement = formatdoc!(
					"
						update objects
						set children = 1
						where id = {p}1;
					"
				);
				let params = db::params![id];
				transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				// Commit the transaction.
				match transaction.commit().await {
					Ok(()) => break,
					Err(error) if error.is_retry() => continue,
					Err(source) => return Err(tg::error!(!source, "failed to commit transaction")),
				}
			}
		}

		if count.is_none() {
			// Attempt to compute the count.
			let count = children_metadata
				.iter()
				.map(|option| option.as_ref().and_then(|metadata| metadata.count))
				.chain(std::iter::once(Some(1)))
				.sum::<Option<u64>>();

			// Set the count if possible.
			if count.is_some() {
				// Set the count.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update objects
						set count = {p}1
						where id = {p}2;
					"
				);
				let params = db::params![count, id];
				connection
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		// Attempt to set the weight if necessary.
		if weight.is_none() {
			// Attempt to compute the weight.
			let weight = children_metadata
				.iter()
				.map(|option| option.as_ref().and_then(|metadata| metadata.weight))
				.chain(std::iter::once(Some(bytes.len().to_u64().unwrap())))
				.sum::<Option<u64>>();

			// Set the weight if possible.
			if weight.is_some() {
				let p = connection.p();
				let statement = formatdoc!(
					"
						update objects
						set weight = {p}1
						where id = {p}2;
					"
				);
				let params = db::params![weight, id];
				connection
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		// Attempt to set the complete flag if necessary.
		let completed = if complete {
			false
		} else {
			// Attempt to set the complete flag.
			let p = connection.p();
			let statement = formatdoc!(
				"
					update objects
					set complete = (
						select case
							when count(*) = count(complete) then coalesce(min(complete), 1)
							else 0
						end
						from object_children
						left join objects on objects.id = object_children.child
						where object_children.object = {p}1
					)
					where id = {p}1
					returning complete;
				"
			);
			let params = db::params![id];
			connection
				.query_one_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		};

		// If the object became complete, then index its incomplete parents.
		if completed {
			// Get the incomplete parents.
			let statement = formatdoc!(
				"
					select object
					from object_children
					left join objects on objects.id = object_children.object
					where object_children.child = {p}1 and objects.complete = 0;
				"
			);
			let params = db::params![id];
			let objects = connection
				.query_all_value_into::<tg::object::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			// Index the incomplete parents.
			tokio::spawn({
				let server = self.clone();
				async move {
					server.enqueue_objects_for_indexing(&objects).await.ok();
				}
			});
		}

		// If the object became complete, then index its builds with incomplete logs, outcomes, or targets.
		if completed {
			// Get the incomplete builds.
			let p = connection.p();
			let statement = formatdoc!(
				"
					select build
					from build_objects
					left join builds on builds.id = build_objects.build
					where
						build_objects.object = {p}1 and
						builds.status = 'finished' and (
							(builds.logs_complete = 0 and builds.log = {p}1) or
							builds.outcomes_complete = 0 or
							(builds.targets_complete = 0 and builds.target = {p}1)
						);
				"
			);
			let params = db::params![id];
			let builds = connection
				.query_all_value_into::<tg::build::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Index the incomplete builds that are not queued for indexing.
			tokio::spawn({
				let server = self.clone();
				async move {
					server.enqueue_builds_for_indexing(&builds).await.ok();
				}
			});
		}

		// Set the indexing status to null if it is started.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update objects
				set
					index_status = null,
					index_started_at = null
				where
					id = {p}1 and
					index_status = 'started';
			"
		);
		let params = db::params![id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(())
	}

	pub(crate) async fn enqueue_objects_for_indexing(
		&self,
		objects: &[tg::object::Id],
	) -> tg::Result<()> {
		loop {
			// Get a database connection.
			let mut connection = self
				.database
				.connection(db::Priority::Low)
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			let transaction = connection
				.transaction()
				.await
				.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

			for id in objects {
				// Set the object's indexing status to enqueued.
				let p = transaction.p();
				let statement = formatdoc!(
					"
						update objects
						set
							index_status = 'enqueued',
							index_started_at = null
						where
							id = {p}1 and
							index_status is null;
					"
				);
				let params = db::params![id];
				transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Commit the transaction.
			match transaction.commit().await {
				Ok(()) => break,
				Err(error) if error.is_retry() => continue,
				Err(source) => return Err(tg::error!(!source, "failed to commit the transaction")),
			}
		}

		for id in objects {
			// Publish the object indexing message.
			let subject = "objects.index".to_owned();
			let payload = id.to_string().into();
			self.messenger
				.publish(subject, payload)
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
		}

		Ok(())
	}
}
