use crate::Server;
use bytes::Bytes;
use futures::{future, stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::pin::pin;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
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

		loop {
			// Attempt to get an object to index.
			let connection = match self.database.connection().await {
				Ok(connection) => connection,
				Err(error) => {
					tracing::error!(?error, "failed to get a database connection");
					let duration = std::time::Duration::from_secs(1);
					tokio::time::sleep(duration).await;
					continue;
				},
			};
			let p = connection.p();
			let statement = formatdoc!(
				"
					update objects
					set
						indexing_status = 'started',
						indexing_started_at = {p}1
					where id in (
						select id
						from objects
						where
							indexing_status = 'enqueued' or
							(indexing_status = 'started' and indexing_started_at <= {p}2)
						limit 1
					)
					returning id;
				"
			);
			let now = (time::OffsetDateTime::now_utc()).format(&Rfc3339).unwrap();
			let time = (time::OffsetDateTime::now_utc() - std::time::Duration::from_secs(60))
				.format(&Rfc3339)
				.unwrap();
			let params = db::params![now, time];
			let result = connection
				.query_optional_value_into::<tg::object::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"));
			drop(connection);
			let id = match result {
				Ok(Some(id)) => id,

				// If there are no objects enqueued for indexing, then wait to receive an object indexing event or for a timeout to pass.
				Ok(None) => {
					let timeout = std::time::Duration::from_secs(60);
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

			// Spawn a task to index the object.
			tokio::spawn({
				let server = self.clone();
				async move {
					server
						.index_object(&id)
						.await
						.inspect_err(|error| tracing::error!(?error))
						.ok();
				}
			});
		}
	}

	async fn index_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object.
		#[derive(serde::Deserialize)]
		struct Row {
			bytes: Bytes,
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

		// Drop the connection.
		drop(connection);

		// Deserialize the object.
		let data = tg::object::Data::deserialize(id.kind(), &bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;

		// If the children were not set, then add them.
		if !children {
			// Get a database connection.
			let mut connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Begin a transaction.
			let transaction = connection
				.transaction()
				.await
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

			// Add the children.
			for child in data.children() {
				let p = transaction.p();
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
			let p = transaction.p();
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
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

			// Drop the connection.
			drop(connection);
		}

		// Get the children's metadata.
		let children_metadata: Vec<Option<tg::object::Metadata>> = data
			.children()
			.into_iter()
			.map(|id| async move { self.try_get_object_metadata(&id).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Attempt to set the count if necessary.
		if count.is_none() {
			// Attempt to compute the count.
			let count = children_metadata
				.iter()
				.map(|option| option.as_ref().and_then(|metadata| metadata.count))
				.chain(std::iter::once(Some(1)))
				.sum::<Option<u64>>();

			// Set the count if possible.
			if count.is_some() {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

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

				// Drop the connection.
				drop(connection);
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
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the weight.
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

				// Drop the connection.
				drop(connection);
			}
		}

		// Attempt to set the complete flag if necessary.
		let completed = if complete {
			false
		} else {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

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
			let completed = connection
				.query_one_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			completed
		};

		// If the object became complete, then index its incomplete parents.
		if completed {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Get the incomplete parents.
			let p = connection.p();
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

			// Drop the connection.
			drop(connection);

			// Index the incomplete parents.
			for object in objects {
				self.enqueue_object_for_indexing(&object).await?;
			}
		}

		// If the object became complete, then index its builds with incomplete logs, outcomes, or targets.
		if completed {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

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

			// Drop the connection.
			drop(connection);

			// Index the incomplete builds.
			for build in builds {
				self.enqueue_build_for_indexing(&build).await?;
			}
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Set the indexing status to null if it is started.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update objects
				set
					indexing_status = null,
					indexing_started_at = null
				where
					id = {p}1 and
					indexing_status = 'started';
			"
		);
		let params = db::params![id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		Ok(())
	}

	pub(crate) async fn enqueue_object_for_indexing(&self, id: &tg::object::Id) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Set the object's indexing status to enqueued.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update objects
				set
					indexing_status = 'enqueued',
					indexing_started_at = null
				where id = {p}1;
			"
		);
		let params = db::params![id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Publish the object indexing message.
		let subject = "objects.index".to_owned();
		let payload = id.to_string().into();
		self.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}
}
