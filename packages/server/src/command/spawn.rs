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
	pub async fn try_spawn_command(
		&self,
		id: &tg::command::Id,
		arg: tg::command::spawn::Arg,
	) -> tg::Result<Option<tg::command::spawn::Output>> {
		// If the remote arg was set, then process the target remotely.
		if let Some(name) = arg.remote.as_ref() {
			let remote = self.get_remote_client(name.clone()).await?;
			let arg = tg::command::spawn::Arg {
				remote: None,
				..arg
			};
			let output = remote.try_spawn_command(id, arg).await?;
			let output = output.map(|mut output| {
				output.remote.replace(name.to_owned());
				output
			});
			return Ok(output);
		}

		// Perform cycle detection.
		if let Some(parent) = arg.parent.as_ref() {
			let cycle = self.detect_process_cycle(parent, id).await?;
			if cycle {
				return Err(tg::error!("cycle detected"));
			}
		}

		// Perform overflow detection.
		if let Some(parent) = arg.parent.as_ref() {
			let overflow = self.detect_process_overflow(parent).await?;
			if overflow {
				return Err(tg::error!("overflow detected"));
			}
		}

		// Get the command.
		let command = tg::Command::with_id(id.clone());

		// Get a local process if one exists that satisfies the retry constraint.
		'a: {
			// Don't look for cache hits if the command is not cacheable.
			if command.cacheable(self).await? {
				break 'a;
			}

			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Attempt to get a process for the target.
			#[derive(serde::Deserialize)]
			struct Row {
				id: tg::process::Id,
				status: tg::process::Status,
			}
			let p = connection.p();
			let statement = formatdoc!(
				"
					select id, status
					from processes
					where
						target = {p}1
					order by enqueued_at desc
					limit 1;
				"
			);
			let params = db::params![id];
			let Some(Row { id, status }) = connection
				.query_optional_into::<Row>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				break 'a;
			};
			let process = tg::Process::with_id(id.clone());

			// Drop the connection.
			drop(connection);

			// If the process is finished, then verify that the process's output satisfies the retry constraint.
			if status.is_finished() {
				let output = self.get_process(&id).await?;
				if let Some(retry) = output.status.retry() {
					if retry && arg.retry {
						break 'a;
					}
				}
			}

			// Attempt to add the process as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.try_add_process_child(parent, process.id()).await.map_err(
					|source| tg::error!(!source, %parent, %child = process.id(), "failed to add the process as a child"),
				)?;
			}

			// Touch the process.
			tokio::spawn({
				let server = self.clone();
				let process = process.clone();
				async move {
					let arg = tg::process::touch::Arg { remote: None };
					server.touch_process(process.id(), arg).await.ok();
				}
			});

			// Create the output.
			let token = self.try_create_process_token(&id).await?;
			let output = tg::command::spawn::Output {
				process: process.id().clone(),
				remote: None,
				token,			
			};

			return Ok(Some(output));
		}

		// Get a remote process if one exists that satisfies the retry constraint.
		'a: {
			// Find a process.
			let futures = self
				.get_remote_clients()
				.await?
				.into_iter()
				.map(|(name, client)| {
					let arg = arg.clone();
					Box::pin(async move {
						let arg = tg::command::spawn::Arg {
							create: false,
							remote: None,
							..arg.clone()
						};
						let mut output =
							client.spawn_command(id, arg).await?;
						output.remote.replace(name);
						Ok::<_, tg::Error>(Some((output, client)))
					})
				})
				.collect_vec();

			// Wait for the first process.
			if futures.is_empty() {
				break 'a;
			}
			let Ok((Some((output, _remote)), _)) = future::select_ok(futures).await else {
				break 'a;
			};

			// Add the process as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.try_add_process_child(parent, &output.process)
					.await
					.map_err(
						|source| tg::error!(!source, %parent, %child = output.process, "failed to add process as a child"),
					)?;
			}

			// Touch the process.
			tokio::spawn({
				let server = self.clone();
				let output = output.clone();
				async move {
					let arg = tg::process::touch::Arg { remote: None };
					server.touch_process(&output.process, arg).await.ok();
				}
			});

			return Ok(Some(output));
		};

		// If the create arg is false, then return `None`.
		if !arg.create {
			return Ok(None);
		}

		// Otherwise, create a new process.
		let process_id = tg::process::Id::new();

		// Get the host.
		let target = tg::Command::with_id(id.clone());
		let host = target.host(self).await?;

		// Put the process.
		let put_arg = tg::process::put::Arg {
			id: process_id.clone(),
			children: Vec::new(),
			depth: 1,
			error: None,
			host: host.clone(),
			log: None,
			output: None,
			retry: arg.retry,
			status: tg::process::Status::Enqueued,
			command: id.clone(),
			created_at: time::OffsetDateTime::now_utc(),
			enqueued_at: Some(time::OffsetDateTime::now_utc()),
			dequeued_at: None,
			started_at: None,
			finished_at: None,
		};
		self.put_process(&process_id, put_arg).await?;

		// Create the process's log if necessary.
		if !self.config.advanced.write_process_logs_to_database {
			let path = self.logs_path().join(process_id.to_string());
			tokio::fs::File::create(&path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to create the log file"),
			)?;
		}

		// Add the process to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.try_add_process_child(parent, &process_id)
				.await
				.map_err(
					|source| tg::error!(!source, %parent, %child = process_id, "failed to add process as a child"),
				)?;
		}

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("processes.created".to_owned(), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		// Spawn a task to spawn the process when the parent's permit is available.
		let server = self.clone();
		let parent = arg.parent.clone();
		let process = process_id.clone();
		tokio::spawn(async move {
			// Acquire the parent's permit.
			let Some(permit) = parent.as_ref().and_then(|parent| {
				server
					.process_permits
					.get(parent)
					.map(|permit| permit.clone())
			}) else {
				return;
			};
			let permit = permit
				.lock_owned()
				.map(|guard| BuildPermit(Either::Right(guard)))
				.await;

			// Attempt to spawn the process.
			server
				.spawn_process(process, permit, None)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to spawn the process"))
				.ok();
		});

		// Create a token for the process.
		let token = self.try_create_process_token(&process_id).await?;

		// Return the output.
		let output = tg::command::spawn::Output {
			process: process_id,
			remote: None,
			token,
		};
		Ok(Some(output))
	}

	async fn detect_process_cycle(
		&self,
		parent: &tg::process::Id,
		target: &tg::command::Id,
	) -> tg::Result<bool> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// First check for a self-cycle.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select exists (
					select 1 from processes
					where id = {p}1 and target = {p}2
				);
			"
		);

		let params = db::params![parent, target];
		let cycle = connection
			.query_one_value_into(statement.into(), params)
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
					from processes b
					join process_children c on b.id = c.child
					where c.child = {p}1

					union all

					select b.id, b.target
					from ancestors a
					join process_children c on a.id = c.child
					join processes b on c.process = b.id
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
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute statement"))?;

		Ok(cycle)
	}

	async fn detect_process_overflow(&self, parent: &tg::process::Id) -> tg::Result<bool> {
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let p = connection.p();

		#[derive(serde::Deserialize, serde::Serialize)]
		struct Row {
			id: tg::process::Id,
			depth: u64,
		}
		let statement = formatdoc!(
			"
				with recursive ancestors as (
					select b.id, b.depth
					from processes b
					join process_children c on b.id = c.child
					where c.child = {p}1

					union all

					select b.id, b.depth
					from ancestors a
					join process_children c on a.id = c.child
					join processes b on c.process = b.id
				)
				select id, depth from ancestors;
			"
		);
		let params = db::params![parent];
		let ancestors = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let max_depth = ancestors.iter().map(|row| row.depth).max();
		let ancestors = ancestors.iter().map(|row| row.id.clone()).collect_vec();
		let ancestors = serde_json::to_string(&ancestors).unwrap();
		if let Some(max_depth) = max_depth {
			if max_depth >= self.config.process.as_ref().unwrap().max_depth {
				return Ok(true);
			}
			let statement = formatdoc!(
				"
					update processes
					set depth = depth + 1
					where id in (select value from json_each({p}1));
				"
			);
			let params = db::params![ancestors];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the connection.
		drop(connection);

		Ok(false)
	}
}

impl Server {
	pub(crate) async fn handle_spawn_command_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.try_spawn_command(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
