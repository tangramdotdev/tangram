use crate::{ProcessPermit, Server, database};
use bytes::Bytes;
use futures::{FutureExt as _, future};
use indoc::formatdoc;
use std::{path::PathBuf, pin::pin};
use tangram_client::{self as tg, prelude::*};
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::prelude::*;

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

		// Get the command.
		let command = arg.command.as_ref().unwrap();

		// Determine if the process is cacheable.
		let cacheable = arg.checksum.is_some()
			|| (arg.mounts.is_empty()
				&& !arg.network
				&& arg.stdin.is_none()
				&& arg.stdout.is_none()
				&& arg.stderr.is_none());

		// If the process is not cacheable or cached is false, then spawn a local process, add it as a child of the parent, and return it.
		if !cacheable || matches!(arg.cached, Some(false)) {
			let (id, token) = self.spawn_local_process(&arg).await?;
			if let Some(parent) = arg.parent.as_ref() {
				self.try_add_process_child(
					parent,
					&id,
					command.options.path.as_ref(),
					command.options.tag.as_ref(),
					Some(&token),
				)
				.await
				.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
			}
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
				token: Some(token),
			};
			return Ok(Some(output));
		}

		// Return a cached local process if one exists.
		if let Some((id, token)) = self.try_get_cached_process_local(&arg).await? {
			if let Some(parent) = arg.parent.as_ref() {
				self.try_add_process_child(
					parent,
					&id,
					command.options.path.as_ref(),
					command.options.tag.as_ref(),
					token.as_ref(),
				)
				.await
				.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
			}
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
				token,
			};
			return Ok(Some(output));
		}

		// Return a cached local process with mismatched checksum if possible.
		if let Some(id) = self
			.try_get_cached_process_with_mismatched_checksum_local(&arg)
			.await?
		{
			if let Some(parent) = arg.parent.as_ref() {
				self.try_add_process_child(
					parent,
					&id,
					command.options.path.as_ref(),
					command.options.tag.as_ref(),
					None,
				)
				.await
				.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
			}
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
				token: None,
			};
			return Ok(Some(output));
		}

		// If cached is true, then attempt to get a remote process, and return none if none is found.
		if matches!(arg.cached, Some(true)) {
			if let Some(output) = self.try_get_cached_process_remote(&arg).await? {
				if let Some(parent) = arg.parent.as_ref() {
					self.try_add_process_child(
					parent,
					&output.process,
					command.options.path.as_ref(),
					command.options.tag.as_ref(),
					output.token.as_ref(),
				)
				.await
				.map_err(
					|source| tg::error!(!source, %parent, %child = output.process, "failed to add the process as a child"),
				)?;
				}
				return Ok(Some(output));
			}
			return Ok(None);
		}

		// Spawn a local process.
		let (local_id, local_token) = self.spawn_local_process(&arg).await?;

		// Create a future that will await the local process.
		let local = async { self.wait_process(&local_id).await };

		// Create a future that will attempt to get a cached remote process.
		let remote = async { self.try_get_cached_process_remote(&arg).await };

		// If the local process finishes before the remote responds, then use the local process. If a remote process is found sooner, then spawn a task to cancel the local process and use the remote process.
		let (id, token) = match future::select(pin!(local), pin!(remote)).await {
			future::Either::Left((_, _)) => (local_id.clone(), Some(local_token.clone())),
			future::Either::Right((remote, _)) => {
				if let Ok(Some(output)) = remote {
					tokio::spawn({
						let server = self.clone();
						let id = local_id.clone();
						let token = local_token.clone();
						async move {
							let arg = tg::process::cancel::Arg {
								remote: output.remote,
								token,
							};
							server.cancel_process(&id, arg).boxed().await.ok();
						}
					});
					(output.process, output.token)
				} else {
					(local_id.clone(), Some(local_token.clone()))
				}
			},
		};

		// Add the process to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.try_add_process_child(
				parent,
				&id,
				command.options.path.as_ref(),
				command.options.tag.as_ref(),
				token.as_ref(),
			)
			.await
			.map_err(
				|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
			)?;
		}

		// Create the output.
		let output = tg::process::spawn::Output {
			process: id,
			remote: None,
			token,
		};

		Ok(Some(output))
	}

	async fn try_get_cached_process_local(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<(tg::process::Id, Option<String>)>> {
		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();

		// Get the command.
		let command = arg.command.as_ref().unwrap().clone();

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Attempt to get a matching process.
		#[derive(serde::Deserialize)]
		struct Row {
			id: tg::process::Id,
			error: Option<db::value::Json<tg::Error>>,
			exit: Option<u8>,
			status: tg::process::Status,
		}
		let params = match &transaction {
			database::Transaction::Sqlite(_) => {
				"with params as (select ?1 as command, ?2 as checksum)"
			},
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => {
				"with params as (select $1::text as command, $2::text as checksum)"
			},
		};
		let is = match &transaction {
			database::Transaction::Sqlite(_) => "is",
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "is not distinct from",
		};
		let isnt = match &transaction {
			database::Transaction::Sqlite(_) => "is not",
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "is distinct from",
		};
		let statement = formatdoc!(
			"
				{params}
				select id, error, exit, status
				from processes, params
				where
					processes.command = params.command and
					processes.cacheable = 1 and
					processes.expected_checksum {is} params.checksum and
					processes.error_code {isnt} 'cancellation'
				order by processes.created_at desc
				limit 1;
			"
		);
		let params = db::params![command.item, arg.checksum];
		let Some(Row {
			id,
			error,
			exit,
			status,
		}) = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// If the process failed and the retry flag is set, then return.
		let failed = error.is_some() || exit.is_some_and(|exit| exit != 0);
		if failed && arg.retry {
			return Ok(None);
		}

		// If the process is not finished, then create a process token.
		let token = if status == tg::process::Status::Finished {
			None
		} else {
			let token = Self::create_process_token();
			let statement = formatdoc!(
				"
					insert into process_tokens (process, token)
					values ({p}1, {p}2);
				"
			);
			let params = db::params![id, token];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			Some(token)
		};

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		// Touch the process.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				let arg = tg::process::touch::Arg { remote: None };
				server.touch_process(&id, arg).await.ok();
			}
		});

		Ok(Some((id, token)))
	}

	async fn try_get_cached_process_with_mismatched_checksum_local(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::Id>> {
		// Get the command.
		let command = arg.command.as_ref().unwrap();

		// If the checksum is not set, then return.
		let Some(expected_checksum) = arg.checksum.clone() else {
			return Ok(None);
		};

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Attempt to get a process.
		#[derive(serde::Deserialize)]
		struct Row {
			id: tg::process::Id,
			actual_checksum: tg::Checksum,
			output: Option<db::value::Json<tg::value::Data>>,
		}
		let p = connection.p();
		let params = match &connection {
			database::Connection::Sqlite(_) => {
				"with params as (select ?1 as command, ?2 as checksum)"
			},
			#[cfg(feature = "postgres")]
			database::Connection::Postgres(_) => {
				"with params as (select $1::text as command, $2::text as checksum)"
			},
		};
		let is = match &connection {
			database::Connection::Sqlite(_) => "is",
			#[cfg(feature = "postgres")]
			database::Connection::Postgres(_) => "is not distinct from",
		};
		let statement = formatdoc!(
			"
				{params}
				select id, actual_checksum, output
				from processes, params
				where
					processes.command = params.command and
					processes.cacheable = 1 and
					processes.error_code {is} 'checksum_mismatch' and
					processes.actual_checksum is not null and
					split_part(processes.actual_checksum, ':', 1) = split_part(params.checksum, ':', 1)
				order by processes.created_at desc
				limit 1;
			"
		);
		let params = db::params![command.item, expected_checksum];
		let Some(Row {
			id: existing_id,
			actual_checksum,
			output,
		}) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Drop the connection.
		drop(connection);

		// Set the exit, output, and error.
		let (exit, error) = if expected_checksum == actual_checksum {
			(0, None)
		} else {
			let expected = &expected_checksum;
			let actual = &actual_checksum;
			let error = tg::error!(
				code = tg::error::Code::ChecksumMismatch,
				%expected,
				%actual,
				"checksum mismatch",
			);
			(1, Some(error))
		};

		// Touch the command.
		self.touch_object(
			&command.item.clone().into(),
			tg::object::touch::Arg::default(),
		)
		.await?;

		// Create an ID.
		let id = tg::process::Id::new();

		// Get the host.
		let command_ = tg::Command::with_id(command.item.clone());
		let host = &*command_.host(self).await?;

		// Copy the log file.
		let src = self.logs_path().join(existing_id.to_string());
		let dst = self.logs_path().join(id.to_string());
		tokio::fs::copy(src, dst).await.ok();

		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Insert the process children.
		let statement = formatdoc!(
			"
				insert into process_children (process, position, child, path, tag)
				select process, position, child, path, tag from process_children where process = {p}1;
			"
		);
		let params = db::params![id];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the process.
		let statement = formatdoc!(
			"
				insert into processes (
					id,
					actual_checksum,
					cacheable,
					command,
					created_at,
					error,
					error_code,
					exit,
					expected_checksum,
					finished_at,
					host,
					mounts,
					network,
					output,
					retry,
					status,
					token_count,
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
					{p}15,
					{p}16,
					{p}17,
					{p}18
				)
				on conflict (id) do update set
					actual_checksum = {p}2,
					cacheable = {p}3,
					command = {p}4,
					created_at = {p}5,
					error = {p}6,
					error_code = {p}7,
					exit = {p}8,
					expected_checksum = {p}9,
					finished_at = {p}10,
					host = {p}11,
					mounts = {p}12,
					network = {p}13,
					output = {p}14,
					retry = {p}15,
					status = {p}16,
					token_count = {p}17,
					touched_at = {p}18;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![
			id,
			actual_checksum,
			true,
			command.item,
			now,
			error.as_ref().map(tg::Error::to_data).map(db::value::Json),
			error.as_ref().and_then(|error| error.code),
			exit,
			expected_checksum,
			now,
			host,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			output,
			arg.retry,
			tg::process::Status::Finished,
			0,
			now,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(Some(id))
	}

	async fn spawn_local_process(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<(tg::process::Id, String)> {
		// Get the command.
		let command = arg.command.as_ref().unwrap();

		// Touch the command.
		self.touch_object(
			&command.item.clone().into(),
			tg::object::touch::Arg::default(),
		)
		.await?;

		// Create an ID.
		let id = tg::process::Id::new();

		// Create a token.
		let token = Self::create_process_token();

		// Determine if the process is cacheable.
		let cacheable = arg.checksum.is_some()
			|| (arg.mounts.is_empty()
				&& !arg.network
				&& arg.stdin.is_none()
				&& arg.stdout.is_none()
				&& arg.stderr.is_none());

		// Create the log file.
		let path = self.logs_path().join(id.to_string());
		tokio::fs::File::create(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the log file"))?;

		// Get the host.
		let command_ = tg::Command::with_id(command.item.clone());
		let host = &*command_.host(self).await?;

		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Insert the process.
		let statement = formatdoc!(
			"
				insert into processes (
					id,
					cacheable,
					command,
					created_at,
					enqueued_at,
					expected_checksum,
					host,
					mounts,
					network,
					retry,
					status,
					stderr,
					stdin,
					stdout,
					token_count,
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
					{p}15,
					{p}16
				)
				on conflict (id) do update set
					cacheable = {p}2,
					command = {p}3,
					created_at = {p}4,
					enqueued_at = {p}5,
					expected_checksum = {p}6,
					host = {p}7,
					mounts = {p}8,
					network = {p}9,
					retry = {p}10,
					status = {p}11,
					stderr = {p}12,
					stdin = {p}13,
					stdout = {p}14,
					token_count = {p}15,
					touched_at = {p}16;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![
			id,
			cacheable,
			command.item,
			now,
			now,
			arg.checksum,
			host,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			arg.retry,
			tg::process::Status::Enqueued,
			arg.stderr,
			arg.stdin,
			arg.stdout,
			0,
			now,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the process token.
		let statement = formatdoc!(
			"
				insert into process_tokens (process, token)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![id, token];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		// Publish the process created message.
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

			// Attempt to start the process.
			let arg = tg::process::start::Arg {
				remote: process.remote().cloned(),
			};
			let result = server.start_process(process.id(), arg.clone()).await;
			if let Err(error) = result {
				tracing::trace!(?error, "failed to start the process");
				return;
			}

			// Spawn the process task.
			server.spawn_process_task(&process, permit);
		});

		Ok((id, token))
	}

	async fn try_get_cached_process_remote(
		&self,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
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
			.collect::<Vec<_>>();

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

		Ok(Some(output))
	}

	async fn try_add_process_child(
		&self,
		parent: &tg::process::Id,
		child: &tg::process::Id,
		path: Option<&PathBuf>,
		tag: Option<&tg::Tag>,
		token: Option<&String>,
	) -> tg::Result<()> {
		// Verify the process is local and started.
		if self.try_get_current_process_status_local(parent).await?
			!= Some(tg::process::Status::Started)
		{
			return Err(tg::error!("failed to find the process"));
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Determine if adding this child process creates a cycle.
		let p = connection.p();
		let statement = formatdoc!(
			"
				with recursive ancestors as (
					select {p}1 as id
					union all
					select process_children.process as id
					from ancestors
					join process_children on ancestors.id = process_children.child
				)
				select exists(
					select 1 from ancestors where id = {p}2
				);
			"
		);
		let params = db::params![parent, child];
		let cycle = connection
			.query_one_value_into::<bool>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the cycle check"))?;

		// If adding this child creates a cycle, return an error.
		if cycle {
			return Err(tg::error!("adding this child process creates a cycle"));
		}

		// Drop the connection.
		drop(connection);

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
				insert into process_children (process, position, child, path, tag, token)
				values ({p}1, (select coalesce(max(position) + 1, 0) from process_children where process = {p}1), {p}2, {p}3, {p}4, {p}5)
				on conflict (process, child) do nothing;
			"
		);
		let params = db::params![parent, child, path, tag, token];
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

		Ok(())
	}

	fn create_process_token() -> String {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
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
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
