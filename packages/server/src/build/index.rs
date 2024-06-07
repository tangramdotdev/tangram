use crate::Server;
use either::Either;
use futures::{future, stream::FuturesUnordered, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use std::pin::pin;
use tangram_client::{self as tg, Handle as _};
use tangram_database::{self as db, prelude::*};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub(crate) async fn build_indexer_task(&self) -> tg::Result<()> {
		// Subscribe to build indexing events.
		let mut events = self
			.messenger
			.subscribe("builds.index".to_owned(), Some("queue".to_owned()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.boxed();

		loop {
			// Attempt to get a build to index.
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
					update builds
					set
						index_status = 'started',
						index_started_at = {p}1
					where id in (
						select id
						from builds
						where
							index_status = 'enqueued' or
							(index_status = 'started' and index_started_at <= {p}2)
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
				.query_optional_value_into::<tg::build::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"));
			drop(connection);
			let id = match result {
				Ok(Some(id)) => id,

				// If there are no builds enqueued for indexing, then wait to receive a build indexing event or for a timeout to pass.
				Ok(None) => {
					let timeout = std::time::Duration::from_secs(60);
					let timeout = tokio::time::sleep(timeout);
					future::select(events.next(), pin!(timeout)).await;
					continue;
				},

				Err(error) => {
					tracing::error!(?error, "failed to get a build to index");
					let duration = std::time::Duration::from_secs(1);
					tokio::time::sleep(duration).await;
					continue;
				},
			};

			// Spawn a task to index the build.
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
	}

	async fn index_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		// Get the build.
		let build = self.get_build(id).await?;

		// Ensure the build is finished.
		if build.status != tg::build::Status::Finished {
			return Err(tg::error!("expected the build to be finished"));
		}

		// Get the children.
		let arg = tg::build::children::get::Arg::default();
		let children = self
			.get_build_children(id, arg)
			.await?
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.flat_map(|chunk| chunk.data)
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
		#[allow(clippy::struct_excessive_bools)]
		#[derive(serde::Deserialize)]
		struct Row {
			complete: bool,
			logs_complete: bool,
			outcomes_complete: bool,
			targets_complete: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					complete,
					logs_complete,
					outcomes_complete,
					targets_complete
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let Row {
			complete,
			logs_complete,
			outcomes_complete,
			targets_complete,
		} = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Attempt to mark the build complete if necessary.
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
					update builds
					set complete = (
						select case
							when count(*) = count(complete) then coalesce(min(complete), 1)
							else 0
						end
						from build_children
						left join builds on builds.id = build_children.child
						where build_children.build = {p}1
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

		// If the build became complete, then add its finished incomplete parents to the queue.
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
					select build
					from build_children
					left join builds on builds.id = build_children.build
					where
						build_children.child = {p}1 and
						builds.status = 'finished' and
						builds.complete = 0;
				"
			);
			let params = db::params![id];
			let builds = connection
				.query_all_value_into::<tg::build::Id>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			// Index the incomplete parents.
			for build in builds {
				self.enqueue_build_for_indexing(&build).await?;
			}
		}

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
			let bool_cast = match &connection {
				Either::Left(_) => "",
				Either::Right(_) => "::bool",
			};
			let integer_cast = match &connection {
				Either::Left(_) => "",
				Either::Right(_) => "::integer",
			};
			let statement = formatdoc!(
				"
					update builds
					set logs_complete = (select (
						select coalesce((
							select complete
							from objects
							where id = (
								select log
								from builds
								where id = {p}1
							)
						), 0)
					){bool_cast} and (
						select case
							when count(*) = count(logs_complete) then coalesce(min(logs_complete), 1)
							else 0
						end
						from build_children
						left join builds on builds.id = build_children.child
						where build_children.build = {p}1
					){bool_cast}){integer_cast}
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

			// Index the parents with incomplete logs.
			for build in builds {
				self.enqueue_build_for_indexing(&build).await?;
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
			let outcome_objects = build
				.outcome
				.as_ref()
				.ok_or_else(|| tg::error!("expected the outcome to be set"))?
				.try_unwrap_succeeded_ref()
				.ok()
				.map(tg::value::Data::children)
				.into_iter()
				.flatten()
				.collect::<Vec<_>>();
			let outcome_objects = serde_json::to_string(&outcome_objects).unwrap();
			let outcome_objects_query = match &connection {
				Either::Left(_) => format!("select value from json_each({p}1)"),
				Either::Right(_) => {
					format!("select value from json_array_elements_text({p}1::string::jsonb)")
				},
			};
			let bool_cast = match &connection {
				Either::Left(_) => "",
				Either::Right(_) => "::bool",
			};
			let integer_cast = match &connection {
				Either::Left(_) => "",
				Either::Right(_) => "::integer",
			};
			let statement = formatdoc!(
				"
					update builds
					set outcomes_complete = (select (
						select case
							when count(*) = count(complete) then coalesce(min(complete), 1)
							else 0
						end
						from objects
						where id in ({outcome_objects_query})
					){bool_cast} and (
						select case
							when count(*) = count(outcomes_complete) then coalesce(min(outcomes_complete), 1)
							else 0
						end
						from build_children
						left join builds on builds.id = build_children.child
						where build_children.build = {p}2
					){bool_cast}){integer_cast}
					where id = {p}2
					returning outcomes_complete;
				"
			);
			let params = db::params![outcome_objects, id];
			let outcomes_completed = connection
				.query_one_value_into(statement, params)
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

			// Index the parents with incomplete outcomes.
			for build in builds {
				self.enqueue_build_for_indexing(&build).await?;
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
			let bool_cast = match &connection {
				Either::Left(_) => "",
				Either::Right(_) => "::bool",
			};
			let integer_cast = match &connection {
				Either::Left(_) => "",
				Either::Right(_) => "::integer",
			};
			let statement = formatdoc!(
				"
					update builds
					set targets_complete = (select (
						select coalesce((
							select complete
							from objects
							where id = (
								select target
								from builds
								where id = {p}1
							)
						), 0)
					){bool_cast} and (
						select case
							when count(*) = count(targets_complete) then coalesce(min(targets_complete), 1)
							else 0
						end
						from build_children
						left join builds on builds.id = build_children.child
						where build_children.build = {p}1
					){bool_cast}){integer_cast}
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

			// Index the parents with incomplete targets.
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

		// Set the indexing status to null if it is started. If it is enqueued then do not change it.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update builds
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

		// Drop the connection.
		drop(connection);

		Ok(())
	}

	pub(crate) async fn enqueue_build_for_indexing(&self, id: &tg::build::Id) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Set the build's indexing status.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update builds
				set
					index_status = 'enqueued',
					index_started_at = null
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

		// Publish the build indexing message.
		let subject = "builds.index".to_owned();
		let payload = id.to_string().into();
		self.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}
}
