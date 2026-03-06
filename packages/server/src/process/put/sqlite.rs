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
		database: &db::sqlite::Database,
		touched_at: i64,
	) -> tg::Result<()> {
		Self::put_process_batch_sqlite(&[(id, &arg.data)], database, touched_at).await
	}

	pub(crate) async fn put_process_batch_sqlite(
		items: &[(&tg::process::Id, &tg::process::Data)],
		database: &db::sqlite::Database,
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

		// Get a database connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

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
					mounts,
					network,
					output,
					pty,
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
					?24
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
					mounts = ?13,
					network = ?14,
					output = ?15,
					pty = ?16,
					retry = ?17,
					started_at = ?18,
					status = ?19,
					stderr = ?20,
					stdin = ?21,
					stdout = ?22,
					token_count = ?23,
					touched_at = ?24
			"
		);
		let mut process_stmt = cache
			.get(&transaction, process_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare the children insert statement.
		let children_statement = indoc!(
			"
				insert into process_children (process, position, child, options)
				values (?1, ?2, ?3, ?4)
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
			let mounts_json =
				(!data.mounts.is_empty()).then(|| serde_json::to_string(&data.mounts).unwrap());
			let output_json = data
				.output
				.as_ref()
				.map(|output| serde_json::to_string(output).unwrap());
			let pty_json = data
				.pty
				.as_ref()
				.map(|pty| serde_json::to_string(pty).unwrap());

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
				mounts_json,
				data.network,
				output_json,
				pty_json,
				data.retry,
				data.started_at,
				data.status.to_string(),
				(!data.stderr.is_null()).then(|| data.stderr.to_string()),
				(!data.stdin.is_null()).then(|| data.stdin.to_string()),
				(!data.stdout.is_null()).then(|| data.stdout.to_string()),
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
						child.item.to_string(),
						serde_json::to_string(child.options()).unwrap(),
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
