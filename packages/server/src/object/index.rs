use crate::Server;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::pin::pin;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_messenger::Messenger as _;

impl Server {
	pub(crate) async fn object_index_task(&self) -> tg::Result<()> {
		let stream = self
			.messenger
			.subscribe("objects.index".to_owned(), Some("queue".to_owned()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?;
		let mut stream = pin!(stream);
		while let Some(message) = { stream.next().await } {
			let id = match std::str::from_utf8(&message.payload) {
				Ok(id) => id,
				Err(error) => {
					tracing::error!(?error);
					continue;
				},
			};
			let id = match id.parse() {
				Ok(id) => id,
				Err(error) => {
					tracing::error!(?error);
					continue;
				},
			};
			tokio::spawn({
				let server = self.clone();
				async move {
					server
						.index_object(&id)
						.await
						.inspect_err(|error| {
							tracing::error!(?error);
						})
						.ok();
				}
			});
		}
		Ok(())
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

		// If the object became complete, then add its incomplete parents to the object index queue.
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

			// Add the incomplete parents to the object index queue.
			for object in objects {
				tokio::spawn({
					let server = self.clone();
					async move {
						let subject = "objects.index".to_owned();
						let payload = object.to_string().into();
						tracing::debug!(?object, "publish");
						server.messenger.publish(subject, payload).await.ok();
					}
				});
			}
		}

		// If the object became complete, then add its builds with incomplete logs, outcomes, or targets to the build index queue.
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

			// Add the incomplete builds to the build index queue.
			for build in builds {
				tokio::spawn({
					let server = self.clone();
					async move {
						let subject = "builds.index".to_owned();
						let payload = build.to_string().into();
						server.messenger.publish(subject, payload).await.ok();
					}
				});
			}
		}

		Ok(())
	}
}
