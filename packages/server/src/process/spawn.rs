use crate::{ProcessPermit, Server};
use bytes::Bytes;
use futures::{FutureExt as _, future};
use indoc::formatdoc;
use itertools::Itertools as _;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

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

		// Determine if the process is cacheable.
		let cacheable = arg.checksum.is_some()
			|| (!arg.network
				&& arg.stdin.is_none()
				&& arg.stdout.is_none()
				&& arg.stderr.is_none());

		// If the process is not cacheable, then spawn a local process, add it as a child of the parent, and return it.
		if !cacheable {
			let id = self.spawn_local_process(&arg).await?;
			if let Some(parent) = arg.parent.as_ref() {
				let added = self.try_add_process_child(parent, &id).await.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
				if !added {
					return Err(tg::error!(%parent, %id, "failed to add child to parent"));
				}
			}
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
			};
			return Ok(Some(output));
		}

		// Return a matching local process if one exists.
		if let Some(id) = self.try_get_cached_process_local(&arg).await? {
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
			};
			return Ok(Some(output));
		}

		// If cached is true, then attempt to get a remote process.
		if matches!(arg.cached, Some(true)) {
			let Some(id) = self.try_get_cached_process_remote(&arg).await? else {
				return Ok(None);
			};
			if let Some(parent) = arg.parent.as_ref() {
				let added = self.try_add_process_child(parent, &id).await.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
				if !added {
					return Err(tg::error!(%parent, %id, "failed to add child to parent"));
				}
			}
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
			};
			return Ok(Some(output));
		}

		// Spawn a local process.
		let local_id = self.spawn_local_process(&arg).await?;

		// Create a future that will await the local process.
		let id = local_id.clone();
		let local = self.wait_process(&id).boxed();

		// Create a future that will attempt to get a cached remote process.
		let remote = self.try_get_cached_process_remote(&arg).boxed();

		// If the local process finishes before the remote responds, then use the local process. If a remote process is found sooner, then spawn a task to cancel the local process and use the remote process.
		let id = match future::select(local, remote).await {
			future::Either::Left((_, _)) => local_id,
			future::Either::Right((remote, _)) => {
				if let Ok(Some(remote_id)) = remote {
					tokio::spawn({
						let server = self.clone();
						async move {
							let arg = tg::process::finish::Arg {
								error: Some(tg::error!(
									code = tg::error::Code::Cancelation,
									"the process was canceled"
								)),
								exit: None,
								output: None,
								remote: None,
							};
							server.try_finish_process(&local_id, arg).boxed().await.ok();
						}
					});
					remote_id
				} else {
					local_id
				}
			},
		};

		// Add the process to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			let added = self.try_add_process_child(parent, &id).await.map_err(
				|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
			)?;
			if !added {
				return Err(tg::error!(%parent, %id, "failed to add child to parent"));
			}
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
			error: Option<db::value::Json<tg::Error>>,
			exit: Option<u8>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id, error, exit
				from processes
				where
					cacheable = 1 and
					command = {p}1 and
					coalesce(checksum = {p}2, checksum is null)
				order by created_at desc
				limit 1;
			"
		);
		let params = db::params![arg.command, arg.checksum];
		let Some(Row { id, error, exit }) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Drop the connection.
		drop(connection);

		// If the process is canceled, then return.
		if error
			.as_ref()
			.is_some_and(|error| matches!(error.0.code, Some(tg::error::Code::Cancelation)))
		{
			return Ok(None);
		}

		// If the process failed and the retry flag is set, then return.
		let failed = error.is_some() || exit.is_some_and(|exit| exit != 0);
		if failed && arg.retry {
			return Ok(None);
		}

		// Attempt to add the process as a child of the parent.
		if let Some(parent) = arg.parent.as_ref() {
			let added = self.try_add_process_child(parent, &id).await.map_err(
				|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
			)?;
			if !added {
				return Err(tg::error!(%parent, %id, "failed to add child to parent"));
			}
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
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<tg::process::Id> {
		// Create an ID.
		let id = tg::process::Id::new();

		// Determine if the process is cacheable.
		let cacheable = arg.checksum.is_some()
			|| (!arg.network
				&& arg.stdin.is_none()
				&& arg.stdout.is_none()
				&& arg.stderr.is_none());

		// Create the log file.
		let path = self.logs_path().join(id.to_string());
		tokio::fs::File::create(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the log file"))?;

		// Get the host.
		let command = tg::Command::with_id(arg.command.clone().unwrap());
		let host = &*command.host(self).await?;

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Insert the process.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into processes (
					id,
					cacheable,
					checksum,
					command,
					created_at,
					enqueued_at,
					host,
					mounts,
					network,
					retry,
					status,
					stderr,
					stdin,
					stdout,
					touched_at
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
					{p}12,
					{p}13,
					{p}14,
					{p}15
				)
				on conflict (id) do update set
					cacheable = {p}2,
					checksum = {p}3,
					command = {p}4,
					created_at = {p}5,
					enqueued_at = {p}6,
					host = {p}7,
					mounts = {p}8,
					network = {p}9,
					retry = {p}10,
					status = {p}11,
					stderr = {p}12,
					stdin = {p}13,
					stdout = {p}14,
					touched_at = {p}15;
			"
		);
		let now = time::OffsetDateTime::now_utc();
		let params = db::params![
			id,
			cacheable,
			arg.checksum,
			arg.command,
			now.format(&Rfc3339).unwrap(),
			now.format(&Rfc3339).unwrap(),
			host,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			arg.retry,
			tg::process::Status::Enqueued,
			arg.stderr,
			arg.stdin,
			arg.stdout,
			now.format(&Rfc3339).unwrap(),
		];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		// Touch the command.
		if let Some(command) = &arg.command {
			let arg = tg::object::touch::Arg { remote: None };
			self.touch_object(&command.clone().into(), arg).await?;
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
		let process = tg::Process::new(id.clone(), None, None, None, None);
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

		Ok(id)
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
						cached: Some(true),
						parent: None,
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

	async fn try_add_process_child(
		&self,
		parent: &tg::process::Id,
		child: &tg::process::Id,
	) -> tg::Result<bool> {
		// Verify the process is local and started.
		if self.try_get_current_process_status_local(parent).await?
			!= Some(tg::process::Status::Started)
		{
			return Ok(false);
		}

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Add the child to the database.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into process_children (process, position, child)
				values ({p}1, (select coalesce(max(position) + 1, 0) from process_children where process = {p}1), {p}2)
				on conflict (process, child) do nothing;
			"
		);
		let params = db::params![parent, child];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			let id = parent.clone();
			async move {
				let subject = format!("processes.{id}.children");
				let payload = Bytes::new();
				server
					.messenger
					.publish(subject, payload)
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(true)
	}

	pub(crate) async fn handle_spawn_process_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.try_spawn_process(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
