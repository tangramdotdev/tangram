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
	pub async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		// If the remote arg was set, then spawn the command remotely.
		if let Some(name) = arg.remote.as_ref() {
			let remote = self.get_remote_client(name.clone()).await?;
			let arg = tg::process::spawn::Arg {
				remote: None,
				..arg
			};
			let output = remote.try_spawn_process(arg).await?;
			let output = output.map(|mut output| {
				output.remote.replace(name.to_owned());
				output
			});
			return Ok(output);
		}

		// Determine if the process is cacheable.
		let sandboxed = arg.cwd.is_none() && arg.env.is_none() && !arg.network;
		let cacheable = arg.checksum.is_some() || sandboxed;

		// If the process is cacheable, then get a matching local process if one exists.
		'a: {
			// Do not look for cache hits if the process is not cacheable.
			if !cacheable {
				break 'a;
			}

			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Attempt to get a matching process.
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
						command = {p}1 and (
							case when {p}2 is not null
							then checksum = {p}2
							else checksum is null
							end
						)
					order by created_at desc
					limit 1;
				"
			);
			let params = db::params![arg.command, arg.checksum];
			let Some(Row { id, status }) = connection
				.query_optional_into::<Row>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				break 'a;
			};

			// Drop the connection.
			drop(connection);

			// If the process is canceled, then break.
			if status.is_canceled() {
				break 'a;
			}

			// If the process is failed and the retry flag is set, then break.
			if status.is_failed() && arg.retry {
				break 'a;
			}

			// Attempt to add the process as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.try_add_process_child(parent, &id).await.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
			}

			// Touch the process.
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				async move {
					let arg = tg::process::touch::Arg { remote: None };
					server.touch_process(&id, arg).await.ok();
				}
			});

			// Create the output.
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
			};

			return Ok(Some(output));
		}

		// If the process is cacheable, then get a remote process if one exists that satisfies the retry constraint.
		'a: {
			if !cacheable {
				break 'a;
			}

			// Find a process.
			let futures = self
				.get_remote_clients()
				.await?
				.into_iter()
				.map(|(name, client)| {
					let arg = arg.clone();
					Box::pin(async move {
						let arg = tg::process::spawn::Arg {
							create: false,
							remote: None,
							..arg.clone()
						};
						let mut output = client.spawn_process(arg).await?;
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
		let id = tg::process::Id::new();

		// Put the process.
		let put_arg = tg::process::put::Arg {
			checksum: arg.checksum,
			children: Vec::new(),
			command: arg.command.unwrap(),
			created_at: time::OffsetDateTime::now_utc(),
			cwd: arg.cwd,
			dequeued_at: None,
			enqueued_at: Some(time::OffsetDateTime::now_utc()),
			env: arg.env,
			error: None,
			finished_at: None,
			id: id.clone(),
			log: None,
			network: arg.network,
			output: None,
			retry: arg.retry,
			started_at: None,
			status: tg::process::Status::Enqueued,
		};
		self.put_process(&id, put_arg).await?;

		// Add the process to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.try_add_process_child(parent, &id).await.map_err(
				|source| tg::error!(!source, %parent, %child = id, "failed to add process as a child"),
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
		let process = tg::Process::new(id.clone(), None, None, None);
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
				.spawn_process_task(&process, permit)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to spawn the process"))
				.ok();
		});

		// Create the output.
		let output = tg::process::spawn::Output {
			process: id,
			remote: None,
		};

		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_spawn_process_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.try_spawn_process(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
