use crate::{BuildPermit, Server};
use bytes::Bytes;
use futures::{future, FutureExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn try_build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> tg::Result<Option<tg::target::build::Output>> {
		// If the remote arg was set, then build the target remotely.
		if let Some(remote) = arg.remote.as_ref() {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::target::build::Arg {
				remote: None,
				..arg
			};
			let output = remote.try_build_target(id, arg).await?;
			return Ok(output);
		}

		// Check if building this target with the specified parent would cause a cycle.
		if let Some(parent) = arg.parent.as_ref() {
			let cycle = self.detect_build_cycle(parent, id).await?;
			if cycle {
				return Err(tg::error!("cycle detected"));
			}
			let depth_overflow = self.detect_depth_overflow(parent).await?;
			if depth_overflow {
				return Err(tg::error!("the build graph is too deep"));
			}
		}

		// Get a local build if one exists that satisfies the retry constraint.
		'a: {
			// Get a database connection.
			let connection = self
				.database
				.connection(db::Priority::Low)
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Attempt to get a build for the target.
			#[derive(serde::Deserialize)]
			struct Row {
				id: tg::build::Id,
				status: tg::build::Status,
			}
			let p = connection.p();
			let statement = formatdoc!(
				"
					select id, status
					from builds
					where
						target = {p}1
					order by created_at desc
					limit 1;
				"
			);
			let params = db::params![id];
			let Some(Row { id, status }) = connection
				.query_optional_into::<Row>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				break 'a;
			};
			let build = tg::Build::with_id(id);

			// Drop the connection.
			drop(connection);

			// If the build is finished, then verify that the build's outcome satisfies the retry constraint.
			if status == tg::build::Status::Finished {
				let outcome = build.get_outcome(self).await?;
				if let Some(outcome) = outcome {
					if outcome.retry() <= arg.retry {
						break 'a;
					}
				}
			}

			// Add the build as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.add_build_child(parent, build.id()).await.map_err(
					|source| tg::error!(!source, %parent, %child = build.id(), "failed to add build as a child"),
				)?;
			}

			// Touch the build.
			tokio::spawn({
				let server = self.clone();
				let build = build.clone();
				async move {
					let arg = tg::build::touch::Arg { remote: None };
					server.touch_build(build.id(), arg).await.ok();
				}
			});

			// Create the output.
			let output = tg::target::build::Output {
				build: build.id().clone(),
			};

			return Ok(Some(output));
		}

		// Get a remote build if one exists that satisfies the retry constraint.
		'a: {
			// Find a build.
			let futures = self
				.remotes
				.iter()
				.map(|remote| {
					let server = self.clone();
					let arg = arg.clone();
					let remote = remote.key().clone();
					Box::pin(async move {
						let arg = tg::target::build::Arg {
							create: false,
							remote: Some(remote.clone()),
							..arg.clone()
						};
						let tg::target::build::Output { build } =
							server.build_target(id, arg).await?;
						let build = tg::Build::with_id(build);
						Ok::<_, tg::Error>(Some((build, remote)))
					})
				})
				.collect_vec();

			// Wait for the first build.
			if futures.is_empty() {
				break 'a;
			}
			let Ok((Some((build, _remote)), _)) = future::select_ok(futures).await else {
				break 'a;
			};

			// Add the build as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.add_build_child(parent, build.id()).await.map_err(
					|source| tg::error!(!source, %parent, %child = build.id(), "failed to add build as a child"),
				)?;
			}

			// Touch the build.
			tokio::spawn({
				let server = self.clone();
				let build = build.clone();
				async move {
					let arg = tg::build::touch::Arg { remote: None };
					server.touch_build(build.id(), arg).await.ok();
				}
			});

			// Create the output.
			let output = tg::target::build::Output {
				build: build.id().clone(),
			};

			return Ok(Some(output));
		};

		// If the create arg is false, then return `None`.
		if !arg.create {
			return Ok(None);
		}

		// Otherwise, create a new build.
		let build_id = tg::build::Id::new();

		// Get the host.
		let target = tg::Target::with_id(id.clone());
		let host = target.host(self).await?;

		// Put the build.
		let put_arg = tg::build::put::Arg {
			id: build_id.clone(),
			children: Vec::new(),
			depth: 1,
			host: host.clone(),
			log: None,
			outcome: None,
			retry: arg.retry,
			status: tg::build::Status::Created,
			target: id.clone(),
			created_at: time::OffsetDateTime::now_utc(),
			dequeued_at: None,
			started_at: None,
			finished_at: None,
		};
		self.put_build(&build_id, put_arg).await?;

		// Create the build.
		let build = tg::Build::with_id(build_id.clone());

		// Create the build's log if necessary.
		if !self.config.advanced.write_build_logs_to_database {
			let path = self.logs_path().join(build_id.to_string());
			tokio::fs::File::create(&path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to create the log file"),
			)?;
		}

		// Add the build to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.add_build_child(parent, build.id()).await.map_err(
				|source| tg::error!(!source, %parent, %child = build.id(), "failed to add build as a child"),
			)?;
		}

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("builds.created".to_owned(), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		// Spawn a task to spawn the build when the parent's permit is available.
		let server = self.clone();
		let parent = arg.parent.clone();
		let build = build.clone();
		tokio::spawn(async move {
			// Acquire the parent's permit.
			let Some(permit) = parent.as_ref().and_then(|parent| {
				server
					.build_permits
					.get(parent)
					.map(|permit| permit.clone())
			}) else {
				return;
			};
			let permit = permit
				.lock_owned()
				.map(|guard| BuildPermit(Either::Right(guard)))
				.await;

			// Attempt to spawn the build.
			server.spawn_build(build, permit, None).await.ok();
		});

		let output = tg::target::build::Output { build: build_id };

		Ok(Some(output))
	}

	async fn detect_depth_overflow(&self, parent: &tg::build::Id) -> tg::Result<bool> {
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let p = connection.p();

		#[derive(serde::Deserialize, serde::Serialize)]
		struct Row {
			id: tg::build::Id,
			depth: u64,
		}
		let statement = formatdoc!(
			"
				with recursive ancestors as (
					select b.id, b.depth
					from builds b
					join build_children c on b.id = c.child
					where c.child = {p}1

					union all

					select b.id, b.depth
					from ancestors a
					join build_children c on a.id = c.child
					join builds b on c.build = b.id
				)
				select id, depth from ancestors;
			"
		);
		let params = db::params![parent];
		let ancestors = connection
			.query_all_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let max_depth = ancestors.iter().map(|row| row.depth).max();
		let ancestors = ancestors.iter().map(|row| row.id.clone()).collect_vec();
		let ancestors = serde_json::to_string(&ancestors).unwrap();
		if let Some(max_depth) = max_depth {
			if max_depth >= 100 {
				return Ok(true);
			}
			let statement = formatdoc!(
				"
					update builds
					set depth = depth + 1
					where id in (select value from json_each({p}1));
				"
			);
			let params = db::params![ancestors];
			connection
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}
		// Drop the connection.
		drop(connection);
		Ok(false)
	}

	async fn detect_build_cycle(
		&self,
		parent: &tg::build::Id,
		target: &tg::target::Id,
	) -> tg::Result<bool> {
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// First check for a self-cycle.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select exists (
					select 1 from builds
					where id = {p}1 and target = {p}2
				);
			"
		);

		let params = db::params![parent, target];
		let cycle = connection
			.query_one_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if cycle {
			return Ok(true);
		}

		// Otherwise, recurse.
		let statement = formatdoc!(
			"
				with recursive ancestors as (
					select b.id, b.target
					from builds b
					join build_children c on b.id = c.child
					where c.child = {p}1

					union all

					select b.id, b.target
					from ancestors a
					join build_children c on a.id = c.child
					join builds b on c.build = b.id
				)
				select exists (
					select 1
					from ancestors
					where target = {p}2
				);
			"
		);
		let params = db::params![parent, target];
		let cycle = connection
			.query_one_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute statement"))?;

		Ok(cycle)
	}
}

impl Server {
	pub(crate) async fn handle_build_target_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.try_build_target(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
