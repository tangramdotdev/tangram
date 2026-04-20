use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn put_process_sqlite(
		id: &tg::process::Id,
		arg: &tg::process::put::Arg,
		process_store: &db::sqlite::Database,
		touched_at: i64,
	) -> tg::Result<()> {
		Self::put_process_batch_sqlite(&[(id, &arg.data)], process_store, touched_at).await
	}

	pub(crate) async fn put_process_batch_sqlite(
		items: &[(&tg::process::Id, &tg::process::Data)],
		process_store: &db::sqlite::Database,
		touched_at: i64,
	) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}

		// Clone items for the closure.
		let items: Vec<_> = items
			.iter()
			.map(|(id, data)| ((*id).clone(), (*data).clone()))
			.collect();

		// Get a process store connection.
		let connection = process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;

		connection
			.with(move |connection, cache| {
				Self::put_process_batch_sqlite_sync(connection, cache, &items, touched_at)
			})
			.await?;

		Ok(())
	}

	pub(crate) fn put_process_batch_sqlite_sync(
		connection: &mut sqlite::Connection,
		cache: &db::sqlite::Cache,
		items: &[(tg::process::Id, tg::process::Data)],
		touched_at: i64,
	) -> tg::Result<()> {
		// Begin a transaction.
		let transaction = connection
			.transaction()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Prepare the process insert statement.
		let process_statement = indoc!(
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
					log,
					output,
					sandbox,
					tty,
					retry,
					started_at,
					status,
					stderr,
					stderr_open,
					stdin,
					stdin_open,
					stdout,
					stdout_open,
					token_count,
					touched_at
				)
				values (
					?1,
					?2,
					?3,
					?4,
					?5,
					?6,
					?7,
					?8,
					?9,
					?10,
					?11,
					?12,
					?13,
					?14,
					?15,
					?16,
					?17,
					?18,
					?19,
					?20,
					?21,
					?22,
					?23,
					?24,
					?25,
					?26
				)
				on conflict (id) do update set
					actual_checksum = ?2,
					cacheable = ?3,
					command = ?4,
					created_at = ?5,
					error = ?6,
					error_code = ?7,
					exit = ?8,
					expected_checksum = ?9,
					finished_at = ?10,
					host = ?11,
					log = ?12,
					output = ?13,
					sandbox = ?14,
					tty = ?15,
					retry = ?16,
					started_at = ?17,
					status = ?18,
					stderr = ?19,
					stderr_open = ?20,
					stdin = ?21,
					stdin_open = ?22,
					stdout = ?23,
					stdout_open = ?24,
					token_count = ?25,
					touched_at = ?26
			"
		);
		let mut process_stmt = cache
			.get(&transaction, process_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare the children insert statement.
		let children_statement = indoc!(
			"
				insert into process_children (process, position, cached, child, options)
				values (?1, ?2, ?3, ?4, ?5)
				on conflict (process, child) do nothing;
			"
		);
		let mut children_stmt = cache
			.get(&transaction, children_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Insert all processes and their children.
		for (id, data) in items {
			let error_string = data.error.as_ref().map(|error| match error {
				tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
				tg::Either::Right(id) => id.to_string(),
			});
			let error_code = data.error.as_ref().and_then(|e| match e {
				tg::Either::Left(data) => data.code.map(|code| code.to_string()),
				tg::Either::Right(_) => None,
			});
			let output_json = data
				.output
				.as_ref()
				.map(|output| serde_json::to_string(output).unwrap());
			let tty_json = data
				.tty
				.as_ref()
				.map(|tty| serde_json::to_string(tty).unwrap());
			let stderr_open = match &data.stderr {
				tg::process::Stdio::Pipe | tg::process::Stdio::Tty => {
					Some(!data.status.is_finished())
				},
				tg::process::Stdio::Blob(_)
				| tg::process::Stdio::Inherit
				| tg::process::Stdio::Log
				| tg::process::Stdio::Null => None,
			};
			let stdin_open = match &data.stdin {
				tg::process::Stdio::Pipe | tg::process::Stdio::Tty => {
					Some(!data.status.is_finished())
				},
				tg::process::Stdio::Blob(_)
				| tg::process::Stdio::Inherit
				| tg::process::Stdio::Log
				| tg::process::Stdio::Null => None,
			};
			let stdout_open = match &data.stdout {
				tg::process::Stdio::Pipe | tg::process::Stdio::Tty => {
					Some(!data.status.is_finished())
				},
				tg::process::Stdio::Blob(_)
				| tg::process::Stdio::Inherit
				| tg::process::Stdio::Log
				| tg::process::Stdio::Null => None,
			};

			let params = sqlite::params![
				id.to_string(),
				data.actual_checksum.as_ref().map(ToString::to_string),
				data.cacheable,
				data.command.to_string(),
				data.created_at,
				error_string,
				error_code,
				data.exit,
				data.expected_checksum.as_ref().map(ToString::to_string),
				data.finished_at,
				data.host,
				data.log.as_ref().map(ToString::to_string),
				output_json,
				data.sandbox.to_string(),
				tty_json,
				data.retry,
				data.started_at,
				data.status.to_string(),
				(!data.stderr.is_null()).then(|| data.stderr.to_string()),
				stderr_open,
				(!data.stdin.is_null()).then(|| data.stdin.to_string()),
				stdin_open,
				(!data.stdout.is_null()).then(|| data.stdout.to_string()),
				stdout_open,
				0,
				touched_at
			];
			process_stmt
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Insert children.
			if let Some(children) = &data.children {
				for (position, child) in children.iter().enumerate() {
					let params = sqlite::params![
						id.to_string(),
						position.to_i64().unwrap(),
						child.cached,
						child.process.to_string(),
						serde_json::to_string(&child.options).unwrap(),
					];
					children_stmt
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}
			}
		}

		// Drop the prepared statements before committing.
		drop(process_stmt);
		drop(children_stmt);

		// Commit the transaction.
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}
}
