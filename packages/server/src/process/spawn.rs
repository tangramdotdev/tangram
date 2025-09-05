use crate::{ProcessPermit, Server, database};
use bytes::Bytes;
use futures::{FutureExt as _, future};
use indoc::formatdoc;
use std::pin::pin;
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

		// Get the host.
		let command_ = tg::Command::with_id(command.item.clone());
		let host = command_.host(self).await?;

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

		// Determine if the process is cacheable.
		let cacheable = arg.checksum.is_some()
			|| (arg.mounts.is_empty()
				&& !arg.network
				&& arg.stdin.is_none()
				&& arg.stdout.is_none()
				&& arg.stderr.is_none());

		// If the process is not cacheable or cached is false, then spawn a local process, add it as a child of the parent, and return it.
		if !cacheable || matches!(arg.cached, Some(false)) {
			// Spawn the process.
			let (id, token, permit) = self
				.spawn_local_process(&transaction, &arg, cacheable, &host)
				.await?;

			// Add the process as a child.
			if let Some(parent) = &arg.parent {
				self.add_process_child_with_transaction(
					&transaction,
					parent,
					&id,
					&command.options,
					Some(&token),
				)
				.await
				.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
			}

			// Commit the transaction.
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

			// Drop the connection.
			drop(connection);

			// If a permit was immediately available, then spawn the process task. Otherwise, spawn tasks to publish the created message and spawn the process task when the parent's permit is available.
			if let Some(permit) = permit {
				let process = tg::Process::new(id.clone(), None, None, None, None);
				self.spawn_process_task(&process, permit);
			} else {
				self.spawn_process_created_message_task();
				self.spawn_process_parent_permit_task(&arg, &id);
			}

			// Publish the child message.
			if let Some(parent) = &arg.parent {
				self.spawn_publish_process_child_message_task(parent);
			}

			// Create the output.
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
				token: Some(token),
			};

			return Ok(Some(output));
		}

		// Return a cached local process if one exists.
		if let Some((id, token)) = self
			.try_get_cached_process_local(&transaction, &arg)
			.await?
		{
			// Add the process as a child.
			if let Some(parent) = &arg.parent {
				self.add_process_child_with_transaction(
					&transaction,
					parent,
					&id,
					&command.options,
					token.as_ref(),
				)
				.await
				.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
			}

			// Commit the transaction.
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

			// Drop the connection.
			drop(connection);

			// Publish the child message.
			if let Some(parent) = &arg.parent {
				self.spawn_publish_process_child_message_task(parent);
			}

			// Create the output.
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
				token,
			};

			return Ok(Some(output));
		}

		// Return a cached local process with mismatched checksum if possible.
		if let Some(id) = self
			.try_get_cached_process_with_mismatched_checksum_local(&transaction, &arg, &host)
			.await?
		{
			// Add the process as a child.
			if let Some(parent) = &arg.parent {
				self.add_process_child_with_transaction(
					&transaction,
					parent,
					&id,
					&command.options,
					None,
				)
				.await
				.map_err(
					|source| tg::error!(!source, %parent, %child = id, "failed to add the process as a child"),
				)?;
			}

			// Commit the transaction.
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

			// Drop the connection.
			drop(connection);

			// Publish the child message.
			if let Some(parent) = &arg.parent {
				self.spawn_publish_process_child_message_task(parent);
			}

			// Create the output.
			let output = tg::process::spawn::Output {
				process: id,
				remote: None,
				token: None,
			};

			return Ok(Some(output));
		}

		// If cached is true, then attempt to get a remote process, and return none if none is found.
		if matches!(arg.cached, Some(true)) {
			// Drop the transaction and connection.
			drop(transaction);
			drop(connection);

			if let Some(output) = self.try_get_cached_process_remote(&arg).await? {
				if let Some(parent) = &arg.parent {
					self.add_process_child(
						parent,
						&output.process,
						&command.options,
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
		let (local_id, local_token, local_permit) = self
			.spawn_local_process(&transaction, &arg, cacheable, &host)
			.await?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		// If a permit was immediately available, then spawn the process task. Otherwise, spawn tasks to publish the created message and spawn the process task when the parent's permit is available.
		if let Some(permit) = local_permit {
			let process = tg::Process::new(local_id.clone(), None, None, None, None);
			self.spawn_process_task(&process, permit);
		} else {
			self.spawn_process_created_message_task();
			self.spawn_process_parent_permit_task(&arg, &local_id);
		}

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

		if let Some(parent) = &arg.parent {
			self.add_process_child(parent, &id, &command.options, token.as_ref())
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
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
	) -> tg::Result<Option<(tg::process::Id, Option<String>)>> {
		let p = transaction.p();

		// Get the command.
		let command = arg.command.as_ref().unwrap().clone();

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
		let params = db::params![
			command.item.to_string(),
			arg.checksum.as_ref().map(ToString::to_string),
		];
		let Some(Row {
			id,
			error,
			exit,
			status,
		}) = transaction
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0)
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
			let params = db::params![id.to_string(), token];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			Some(token)
		};

		Ok(Some((id, token)))
	}

	async fn try_get_cached_process_with_mismatched_checksum_local(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		host: &str,
	) -> tg::Result<Option<tg::process::Id>> {
		let p = transaction.p();

		// Get the command.
		let command = arg.command.as_ref().unwrap();

		// If the checksum is not set, then return.
		let Some(expected_checksum) = arg.checksum.clone() else {
			return Ok(None);
		};

		// Attempt to get a process.
		#[derive(serde::Deserialize)]
		struct Row {
			id: tg::process::Id,
			actual_checksum: tg::Checksum,
			output: Option<db::value::Json<tg::value::Data>>,
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
		let params = db::params![command.item.to_string(), expected_checksum.to_string()];
		let Some(Row {
			id: existing_id,
			actual_checksum,
			output,
		}) = transaction
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0)
		else {
			return Ok(None);
		};

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

		// Create an ID.
		let id = tg::process::Id::new();

		// Copy the log file.
		let src = self.logs_path().join(existing_id.to_string());
		let dst = self.logs_path().join(id.to_string());
		tokio::fs::copy(src, dst).await.ok();

		// Insert the process children.
		let statement = formatdoc!(
			"
				insert into process_children (process, position, child, options)
				select process, position, child, options from process_children where process = {p}1;
			"
		);
		let params = db::params![id.to_string()];
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
			id.to_string(),
			actual_checksum.to_string(),
			true,
			command.item.to_string(),
			now,
			error.as_ref().map(tg::Error::to_data).map(db::value::Json),
			error
				.as_ref()
				.and_then(|error| error.code.map(|code| code.to_string())),
			exit,
			expected_checksum.to_string(),
			now,
			host,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			output,
			arg.retry,
			tg::process::Status::Finished.to_string(),
			0,
			now,
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(Some(id))
	}

	async fn spawn_local_process(
		&self,
		transaction: &database::Transaction<'_>,
		arg: &tg::process::spawn::Arg,
		cacheable: bool,
		host: &str,
	) -> tg::Result<(tg::process::Id, String, Option<ProcessPermit>)> {
		let p = transaction.p();

		// Get the command.
		let command = arg.command.as_ref().unwrap();

		// Create an ID.
		let id = tg::process::Id::new();

		// Create a token.
		let token = Self::create_process_token();

		// Create the log file.
		let path = self.logs_path().join(id.to_string());
		tokio::fs::File::create(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the log file"))?;

		// Attempt to acquire a permit immediately.
		let permit = 'a: {
			if let Some(parent_id) = &arg.parent {
				if let Some(parent_permit) = self.process_permits.get(parent_id) {
					let parent_permit = parent_permit.clone();
					if let Ok(guard) = parent_permit.try_lock_owned() {
						break 'a Some(ProcessPermit(Either::Right(guard)));
					}
				}
			}
			if let Ok(server_permit) = self.process_semaphore.clone().try_acquire_owned() {
				break 'a Some(ProcessPermit(Either::Left(server_permit)));
			}
			None
		};

		// Set the status.
		let status = if permit.is_some() {
			tg::process::Status::Started
		} else {
			tg::process::Status::Enqueued
		};

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
					heartbeat_at,
					host,
					mounts,
					network,
					retry,
					started_at,
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
					{p}16,
					{p}17,
					{p}18
				)
				on conflict (id) do update set
					cacheable = {p}2,
					command = {p}3,
					created_at = {p}4,
					enqueued_at = {p}5,
					expected_checksum = {p}6,
					heartbeat_at = {p}7,
					host = {p}8,
					mounts = {p}9,
					network = {p}10,
					retry = {p}11,
					started_at = {p}12,
					status = {p}13,
					stderr = {p}14,
					stdin = {p}15,
					stdout = {p}16,
					token_count = {p}17,
					touched_at = {p}18;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let heartbeat_at = if permit.is_some() { Some(now) } else { None };
		let started_at = if permit.is_some() { Some(now) } else { None };
		let params = db::params![
			id.to_string(),
			cacheable,
			command.item.to_string(),
			now,
			now,
			arg.checksum.as_ref().map(ToString::to_string),
			heartbeat_at,
			host,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			arg.retry,
			started_at,
			status.to_string(),
			arg.stderr.as_ref().map(ToString::to_string),
			arg.stdin.as_ref().map(ToString::to_string),
			arg.stdout.as_ref().map(ToString::to_string),
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
		let params = db::params![id.to_string(), token];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok((id, token, permit))
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

		Ok(Some(output))
	}

	async fn add_process_child(
		&self,
		parent: &tg::process::Id,
		child: &tg::process::Id,
		options: &tg::referent::Options,
		token: Option<&String>,
	) -> tg::Result<()> {
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

		// Add the process as a child.
		self.add_process_child_with_transaction(&transaction, parent, child, options, token)
			.await
			.map_err(
				|source| tg::error!(!source, %parent, %child, "failed to add the process as a child"),
			)?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		// Publish the child message.
		self.spawn_publish_process_child_message_task(parent);

		Ok(())
	}

	async fn add_process_child_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		parent: &tg::process::Id,
		child: &tg::process::Id,
		options: &tg::referent::Options,
		token: Option<&String>,
	) -> tg::Result<()> {
		let p = transaction.p();

		// Determine if adding this child process creates a cycle.
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
		let params = db::params![parent.to_string(), child.to_string()];
		let cycle = transaction
			.query_one_value_into::<bool>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the cycle check"))?;

		// If adding this child creates a cycle, return an error.
		if cycle {
			return Err(tg::error!("adding this child process creates a cycle"));
		}

		// Add the child to the database.
		let statement = formatdoc!(
			"
				insert into process_children (process, position, child, options, token)
				values ({p}1, (select coalesce(max(position) + 1, 0) from process_children where process = {p}1), {p}2, {p}3, {p}4)
				on conflict (process, child) do nothing;
			"
		);
		let params = db::params![
			parent.to_string(),
			child.to_string(),
			db::value::Json(options),
			token
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(())
	}

	fn spawn_process_created_message_task(&self) {
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
	}

	fn spawn_publish_process_child_message_task(&self, parent: &tg::process::Id) {
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
	}

	fn spawn_process_parent_permit_task(
		&self,
		arg: &tg::process::spawn::Arg,
		id: &tg::process::Id,
	) {
		tokio::spawn({
			let server = self.clone();
			let parent = arg.parent.clone();
			let process = tg::Process::new(id.clone(), None, None, None, None);
			async move {
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
			}
		});
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
