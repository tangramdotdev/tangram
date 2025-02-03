use crate::{ProcessPermit, Server};
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
		// If the remote arg is set, then forward the request.
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

		// Determine if the process is sandboxed.
		let sandboxed = arg.cwd.is_none() && arg.env.is_none() && !arg.network;

		// Determine if the process is cacheable.
		let cacheable = arg.checksum.is_some() || sandboxed;

		// If the process is cacheable, then return a matching local process if one exists.
		if cacheable {
			if let Some(process) = self.try_get_cached_process_local(&arg).await? {
				let output = tg::process::spawn::Output {
					process,
					remote: None,
				};
				return Ok(Some(output));
			}
		}

		// Create an ID for a new local process.
		let id = tg::process::Id::new();

		// Create a future that will spawn and await a local process.
		let local = async {
			if !arg.create {
				return Ok(None);
			}
			self.spawn_local_process(&id, &arg).await?;
			self.wait_process(&id).await?;
			Ok::<_, tg::Error>(Some(id))
		}
		.boxed();

		// Create a future that will attempt to get a cached remote process.
		let remote = async {
			if !cacheable {
				return Ok(None);
			}
			let Some(id) = self.try_get_cached_process_remote(&arg).await? else {
				return Ok(None);
			};
			Ok::<_, tg::Error>(Some(id))
		}
		.boxed();

		// If the local process finishes first, then use it. If a remote process is found sooner, then use it.
		let id = match future::select(local, remote).await {
			future::Either::Left((local, remote)) => {
				if let Ok(Some(id)) = local {
					Some(id)
				} else {
					remote.await?
				}
			},
			future::Either::Right((remote, local)) => {
				if let Ok(Some(id)) = remote {
					let arg = tg::process::finish::Arg {
						error: None,
						exit: None,
						output: None,
						remote: None,
						status: tg::process::Status::Canceled,
					};
					self.try_finish_process(&id, arg).boxed().await.ok();
					Some(id)
				} else {
					local.await?
				}
			},
		};
		let Some(id) = id else {
			return Ok(None);
		};

		// Add the process to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.try_add_process_child(parent, &id).await.map_err(
				|source| tg::error!(!source, %parent, %child = id, "failed to add process as a child"),
			)?;
		}

		// Create the output.
		let output = tg::process::spawn::Output {
			process: id,
			remote: None,
		};

		Ok(Some(output))
	}

	async fn try_get_cached_process_local(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::Id>> {
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
					and cacheable = 1
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
			return Ok(None);
		};

		// Drop the connection.
		drop(connection);

		// If the process is canceled, then return.
		if status.is_canceled() {
			return Ok(None);
		}

		// If the process is failed and the retry flag is set, then return.
		if status.is_failed() && arg.retry {
			return Ok(None);
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

		Ok(Some(id))
	}

	async fn spawn_local_process(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<()> {
		// Put the process.
		let put_arg = tg::process::put::Arg {
			checksum: arg.checksum.clone(),
			children: Vec::new(),
			command: arg.command.clone().unwrap(),
			created_at: time::OffsetDateTime::now_utc(),
			cwd: arg.cwd.clone(),
			dequeued_at: None,
			enqueued_at: Some(time::OffsetDateTime::now_utc()),
			env: arg.env.clone(),
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
		self.put_process(id, put_arg).await?;

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
				.map(|guard| ProcessPermit(Either::Right(guard)))
				.await;

			// Attempt to spawn the process.
			server
				.spawn_process_task(&process, permit)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to spawn the process"))
				.ok();
		});

		Ok(())
	}

	async fn try_get_cached_process_remote(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::Id>> {
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

		// Wait for the first response.
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((Some((output, _remote)), _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		// Touch the process.
		tokio::spawn({
			let server = self.clone();
			let output = output.clone();
			async move {
				let arg = tg::process::touch::Arg { remote: None };
				server.touch_process(&output.process, arg).await.ok();
			}
		});

		Ok(Some(output.process))
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
