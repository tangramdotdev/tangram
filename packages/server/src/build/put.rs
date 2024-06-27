use crate::Server;
use either::Either;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::put::Arg,
	) -> tg::Result<tg::build::put::Output> {
		// Insert the build, build children, and build objects.
		{
			// Get a database connection.
			let mut connection = self
				.database
				.connection(db::Priority::Low)
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Begin a transaction.
			let transaction = connection
				.transaction()
				.await
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			let transaction = Arc::new(transaction);

			// Insert the build.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into builds (
						id,
						host,
						log,
						outcome,
						retry,
						status,
						target,
						touched_at,
						created_at,
						dequeued_at,
						started_at,
						finished_at
					)
					values (
						{p}1,
						{p}2,
						{p}3,
						{p}4,
						{p}5,
						{p}6,
						{p}7,
						{p}8,
						{p}9,
						{p}10,
						{p}11,
						{p}12
					)
					on conflict (id) do update set
						host = {p}2,
						log = {p}3,
						outcome = {p}4,
						retry = {p}5,
						status = {p}6,
						target = {p}7,
						touched_at = {p}8,
						created_at = {p}9,
						dequeued_at = {p}10,
						started_at = {p}11,
						finished_at = {p}12;
				"
			);
			let params = db::params![
				id,
				arg.host,
				arg.log,
				arg.outcome,
				arg.retry,
				arg.status,
				arg.target,
				time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
				arg.created_at.format(&Rfc3339).unwrap(),
				arg.dequeued_at.map(|t| t.format(&Rfc3339).unwrap()),
				arg.started_at.map(|t| t.format(&Rfc3339).unwrap()),
				arg.finished_at.map(|t| t.format(&Rfc3339).unwrap()),
			];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Delete any existing children.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					delete from build_children
					where build = {p}1;
				"
			);
			let params = db::params![id];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Insert the children.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into build_children (build, position, child)
					values ({p}1, {p}2, {p}3);
				"
			);
			arg.children
				.iter()
				.enumerate()
				.map(|(position, child)| {
					let transaction = transaction.clone();
					let statement = statement.clone();
					async move {
						let params = db::params![id, position, child];
						transaction
							.execute(statement, params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						Ok::<_, tg::Error>(())
					}
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;

			// Delete any existing objects.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					delete from build_objects
					where build = {p}1;
				"
			);
			let params = db::params![id];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Insert the objects.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into build_objects (build, object)
					values ({p}1, {p}2)
					on conflict (build, object) do nothing;
				"
			);
			let objects = arg
				.log
				.clone()
				.map(Into::into)
				.into_iter()
				.chain(
					arg.outcome
						.as_ref()
						.and_then(|outcome| outcome.try_unwrap_succeeded_ref().ok())
						.map(tg::value::Data::children)
						.into_iter()
						.flatten(),
				)
				.chain(std::iter::once(arg.target.clone().into()));
			objects
				.map(|object| {
					let transaction = transaction.clone();
					let statement = statement.clone();
					async move {
						let params = db::params![id, object];
						transaction
							.execute(statement, params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						Ok::<_, tg::Error>(())
					}
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;

			// Commit the transaction.
			Arc::into_inner(transaction)
				.unwrap()
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

			// Drop the connection.
			drop(connection);
		}

		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the incomplete flags.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select case
					when builds.log is null then 0
					else case
						when objects.id is null then 1
						else case when objects.complete = 0 then 1 else 0 end
						end
					end
				from builds
				left join objects on objects.id = builds.log
				where builds.id = {p}1;
			"
		);
		let params = db::params![id];
		let log = connection
			.query_one_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let outcome_objects = arg
			.outcome
			.as_ref()
			.map(|outcome| {
				outcome
					.try_unwrap_succeeded_ref()
					.ok()
					.map(tg::value::Data::children)
					.into_iter()
					.flatten()
			})
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
		let statement = formatdoc!(
			"
				select value
				from ({outcome_objects_query})
				left join objects on objects.id = value
				where
					case
						when objects.id is null then true
						else case
							when objects.complete = 0 then true
							else false
						end
					end;
			"
		);
		let params = db::params![outcome_objects];
		let outcome = connection
			.query_all_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.collect();
		let p = connection.p();
		let statement = formatdoc!(
			"
				select case
					when builds.target is null then 0
					else case
						when objects.id is null then 1
						else case when objects.complete = 0 then 1 else 0 end
						end
					end
				from builds
				left join objects on objects.id = builds.target
				where builds.id = {p}1;
			"
		);
		let params = db::params![id];
		let target = connection
			.query_one_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Get the incomplete flags for the children.
		#[allow(clippy::struct_excessive_bools)]
		#[derive(serde::Deserialize)]
		struct ChildRow {
			id: tg::build::Id,
			build: bool,
			logs: bool,
			outcomes: bool,
			targets: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					build_children.child as id,
					case when builds.id is null then 1 else 0 end as build,
					case when builds.logs_complete is null or builds.logs_complete = 0 then 1 else 0 end as logs,
					case when builds.outcomes_complete is null or builds.outcomes_complete = 0 then 1 else 0 end as outcomes,
					case when builds.targets_complete is null or builds.targets_complete = 0 then 1 else 0 end as targets
				from build_children
				left join builds on builds.id = build_children.child
				where build_children.build = {p}1;
			"
		);
		let params = db::params![id];
		let children = connection
			.query_all_into::<ChildRow>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let children = children
			.into_iter()
			.filter_map(|child| {
				let id = child.id;
				let child = tg::build::put::IncompleteChild {
					build: child.build,
					logs: child.logs,
					outcomes: child.outcomes,
					targets: child.targets,
				};
				if child == tg::build::put::IncompleteChild::default() {
					return None;
				}
				Some((id, child))
			})
			.collect();

		// Create the output.
		let output = tg::build::put::Output {
			incomplete: tg::build::put::Incomplete {
				children,
				log,
				outcome,
				target,
			},
		};

		// Drop the database connection.
		drop(connection);

		// If the build is finished, then enqueue the build for indexing.
		if arg.status == tg::build::Status::Finished {
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				async move {
					server.enqueue_builds_for_indexing(&[id]).await.ok();
				}
			});
		}

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_put_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.put_build(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
