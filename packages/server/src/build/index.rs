use crate::Server;
use futures::{stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use std::pin::pin;
use tangram_client::{self as tg, Handle as _};
use tangram_database::{self as db, prelude::*};
use tangram_messenger::Messenger as _;

impl Server {
	pub(crate) async fn build_index_task(&self) -> tg::Result<()> {
		let stream = self
			.messenger
			.subscribe("builds.index".to_owned(), Some("queue".to_owned()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?;
		let mut stream = pin!(stream);
		while let Some(message) = stream.next().await {
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
						.index_build(&id)
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

	async fn index_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		// Get the build.
		let build = self.get_build(id).await?;

		// Ensure the build is finished.
		if build.status != tg::build::Status::Finished {
			return Err(tg::error!("expected the build to be finished"));
		}

		// Get the children.
		let arg = tg::build::children::Arg {
			timeout: Some(std::time::Duration::ZERO),
			..Default::default()
		};
		let children = self
			.get_build_children(id, arg)
			.await?
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.flat_map(|chunk| chunk.items)
			.collect_vec();
		let children = children
			.into_iter()
			.map(|id| async move { self.try_get_build(&id).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		// Attempt to get the log metadata.
		let log_metadata = if let Some(log) = build.log {
			self.try_get_object_metadata(&log.clone().into()).await?
		} else {
			None
		};

		// Attempt to get the outcome metadata.
		let outcome_metadata = if let Ok(output) = build
			.outcome
			.as_ref()
			.ok_or_else(|| tg::error!("expected the outcome to be set"))?
			.try_unwrap_succeeded_ref()
		{
			Some(
				output
					.children()
					.iter()
					.map(|object| self.try_get_object_metadata(object))
					.collect::<FuturesUnordered<_>>()
					.try_collect::<Vec<_>>()
					.await?,
			)
		} else {
			None
		};

		// Attempt to get the target metadata.
		let target_metadata = self
			.try_get_object_metadata(&build.target.clone().into())
			.await?;

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the complete flags.
		#[derive(serde::Deserialize)]
		struct Row {
			logs_complete: bool,
			outcomes_complete: bool,
			targets_complete: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					logs_complete,
					outcomes_complete,
					targets_complete
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let Row {
			logs_complete,
			outcomes_complete,
			targets_complete,
		} = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Attempt to set the count if necessary.
		if build.count.is_none() {
			// Attempt to compute the count.
			let count = children
				.iter()
				.map(|option| option.as_ref().and_then(|output| output.count))
				.chain(std::iter::once(Some(1)))
				.sum::<Option<u64>>();

			// Set the count if possible.
			if let Some(count) = count {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the count and weight.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update builds
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

		// Attempt to set the logs count if necessary.
		if build.logs_count.is_none() {
			// Attempt to get the log count.
			let log_count = log_metadata.as_ref().and_then(|metadata| metadata.count);

			// Attempt to compute the logs count.
			let count = children
				.iter()
				.map(|option| option.as_ref().and_then(|output| output.logs_count))
				.chain(std::iter::once(log_count))
				.sum::<Option<u64>>();

			// Set the logs count if possible.
			if count.is_some() {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the logs count.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update builds
						set logs_count = {p}1
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

		// Attempt to set the logs weight if necessary.
		if build.logs_weight.is_none() {
			// Attempt to get the log weight.
			let log_weight = log_metadata.as_ref().and_then(|metadata| metadata.weight);

			// Attempt to compute the logs weight.
			let weight = children
				.iter()
				.map(|option| option.as_ref().and_then(|output| output.logs_weight))
				.chain(std::iter::once(log_weight))
				.sum::<Option<u64>>();

			// Set the logs weight if possible.
			if weight.is_some() {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the logs weight.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update builds
						set logs_weight = {p}1
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

		// Attempt to mark the logs complete if necessary.
		let logs_completed = if logs_complete {
			false
		} else {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Attempt to set the logs complete flag.
			let p = connection.p();
			let statement = formatdoc!(
				"
					update builds
					set logs_complete = (
						select coalesce((
							select complete
							from objects
							where id = (
								select log
								from builds
								where id = {p}1
							)
						), 0)
					) and (
						select case
							when count(*) = count(logs_complete) then coalesce(min(logs_complete), 1)
							else 0
						end
						from build_children
						left join builds on builds.id = build_children.child
						where build_children.build = {p}1
					)
					where id = {p}1
					returning logs_complete;
				"
			);
			let params = db::params![id];
			let logs_completed = connection
				.query_one_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			logs_completed
		};

		// If the build's logs became complete, then add its finished parents with incomplete logs to the queue.
		if logs_completed {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Get the parents with incomplete logs.
			let p = connection.p();
			let statement = formatdoc!(
				"
					select build
					from build_children
					left join builds on builds.id = build_children.build
					where
						build_children.child = {p}1 and
						builds.status = 'finished' and
						builds.logs_complete = 0;
				"
			);
			let params = db::params![id];
			let builds = connection
				.query_all_value_into::<tg::build::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			// Add the parents with incomplete logs to the build index queue.
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

		// Attempt to set the outcomes count if necessary.
		if build.outcomes_count.is_none() {
			// Attempt to get the outcome count.
			let outcome_count = outcome_metadata.as_ref().and_then(|metadata| {
				metadata
					.iter()
					.map(|metadata| metadata.as_ref().and_then(|metadata| metadata.count))
					.sum::<Option<u64>>()
			});

			// Attempt to compute the outcomes count.
			let count = children
				.iter()
				.map(|option| option.as_ref().and_then(|output| output.outcomes_count))
				.chain(std::iter::once(outcome_count))
				.sum::<Option<u64>>();

			// Set the outcomes count if possible.
			if count.is_some() {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the outcomes count.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update builds
						set outcomes_count = {p}1
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

		// Attempt to set the outcomes weight if necessary.
		if build.outcomes_weight.is_none() {
			// Attempt to get the outcome weight.
			let outcome_weight = outcome_metadata.as_ref().and_then(|metadata| {
				metadata
					.iter()
					.map(|metadata| metadata.as_ref().and_then(|metadata| metadata.weight))
					.sum::<Option<u64>>()
			});

			// Attempt to compute the outcomes weight.
			let weight = children
				.iter()
				.map(|option| option.as_ref().and_then(|output| output.outcomes_weight))
				.chain(std::iter::once(outcome_weight))
				.sum::<Option<u64>>();

			// Set the outcomes weight if possible.
			if weight.is_some() {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the outcomes weight.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update builds
						set outcomes_weight = {p}1
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

		// Attempt to mark the outcomes complete if necessary.
		let outcomes_completed = if outcomes_complete {
			false
		} else {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Attempt to set the outcomes complete flag.
			let p = connection.p();
			connection
				.execute(
					"create temp table t (object text);".to_owned(),
					db::params![],
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let iter = build
				.outcome
				.as_ref()
				.ok_or_else(|| tg::error!("expected the outcome to be set"))?
				.try_unwrap_succeeded_ref()
				.ok()
				.map(tg::value::Data::children)
				.into_iter()
				.flatten();
			for child in iter {
				connection
					.execute(
						format!("insert into t (object) values ({p}1);"),
						db::params![child],
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
			let statement = formatdoc!(
				"
					update builds
					set outcomes_complete = (
						select case
							when count(*) = count(complete) then coalesce(min(complete), 1)
							else 0
						end
						from objects
						where id in (select object from t)
					) and (
						select case
							when count(*) = count(outcomes_complete) then coalesce(min(outcomes_complete), 1)
							else 0
						end
						from build_children
						left join builds on builds.id = build_children.child
						where build_children.build = {p}1
					)
					where id = {p}1
					returning outcomes_complete;
				"
			);
			let params = db::params![id];
			let outcomes_completed = connection
				.query_one_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			connection
				.execute("drop table t;".to_owned(), db::params![])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			outcomes_completed
		};

		// If the build's outcomes became complete, then add its finished parents with incomplete outcomes to the queue.
		if outcomes_completed {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Get the parents with incomplete outcomes.
			let p = connection.p();
			let statement = formatdoc!(
				"
					select build
					from build_children
					left join builds on builds.id = build_children.build
					where 
						build_children.child = {p}1 and
						builds.status = 'finished' and
						builds.outcomes_complete = 0;
				"
			);
			let params = db::params![id];
			let builds = connection
				.query_all_value_into::<tg::build::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			// Add the parents with incomplete outcomes to the build index queue.
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

		// Attempt to set the targets count if necessary.
		if build.targets_count.is_none() {
			// Attempt to get the target count.
			let target_count = target_metadata.as_ref().and_then(|metadata| metadata.count);

			// Attempt to compute the targets count.
			let count = children
				.iter()
				.map(|option| option.as_ref().and_then(|output| output.targets_count))
				.chain(std::iter::once(target_count))
				.sum::<Option<u64>>();

			// Set the targets count if possible.
			if count.is_some() {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the targets count.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update builds
						set targets_count = {p}1
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

		// Attempt to set the targets weight if necessary.
		if build.targets_weight.is_none() {
			// Attempt to get the target weight.
			let target_weight = target_metadata
				.as_ref()
				.and_then(|metadata| metadata.weight);

			// Attempt to compute the targets weight.
			let weight = children
				.iter()
				.map(|option| option.as_ref().and_then(|output| output.targets_weight))
				.chain(std::iter::once(target_weight))
				.sum::<Option<u64>>();

			// Set the targets weight if possible.
			if weight.is_some() {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the targets weight.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update builds
						set targets_weight = {p}1
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

		// Attempt to mark the targets complete if necessary.
		let targets_completed = if targets_complete {
			false
		} else {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Attempt to set the targets complete flag.
			let p = connection.p();
			let statement = formatdoc!(
				"
					update builds
					set targets_complete = (
						select coalesce((
							select complete
							from objects
							where id = (
								select target
								from builds
								where id = {p}1
							)
						), 0)
					) and (
						select case
							when count(*) = count(targets_complete) then coalesce(min(targets_complete), 1)
							else 0
						end
						from build_children
						left join builds on builds.id = build_children.child
						where build_children.build = {p}1
					)
					where id = {p}1
					returning targets_complete;
				"
			);
			let params = db::params![id];
			let targets_completed = connection
				.query_one_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			targets_completed
		};

		// If the build's targets became complete, then add its finished parents with incomplete targets to the queue.
		if targets_completed {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Get the parents with incomplete targets.
			let p = connection.p();
			let statement = formatdoc!(
				"
					select build
					from build_children
					left join builds on builds.id = build_children.build
					where
						build_children.child = {p}1 and
						builds.status = 'finished' and
						builds.targets_complete = 0;
				"
			);
			let params = db::params![id];
			let builds = connection
				.query_all_value_into::<tg::build::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			// Add the parents with incomplete targets to the build index queue.
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
