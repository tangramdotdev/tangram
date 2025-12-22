use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_either::Either,
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
			.with(move |connection| {
				Self::put_process_batch_sqlite_sync(connection, &items, touched_at)
			})
			.await?;

		Ok(())
	}

	pub(crate) fn put_process_batch_sqlite_sync(
		connection: &mut sqlite::Connection,
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
					dequeued_at,
					enqueued_at,
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
					?24,
					?25
				)
				on conflict (id) do update set
					actual_checksum = ?2,
					cacheable = ?3,
					command = ?4,
					created_at = ?5,
					dequeued_at = ?6,
					enqueued_at = ?7,
					error = ?8,
					error_code = ?9,
					exit = ?10,
					expected_checksum = ?11,
					finished_at = ?12,
					host = ?13,
					log = ?14,
					mounts = ?15,
					network = ?16,
					output = ?17,
					retry = ?18,
					started_at = ?19,
					status = ?20,
					stderr = ?21,
					stdin = ?22,
					stdout = ?23,
					token_count = ?24,
					touched_at = ?25
			"
		);
		let mut process_stmt = transaction
			.prepare_cached(process_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare the children insert statement.
		let children_statement = indoc!(
			"
				insert into process_children (process, position, child, options)
				values (?1, ?2, ?3, ?4)
				on conflict (process, child) do nothing;
			"
		);
		let mut children_stmt = transaction
			.prepare_cached(children_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Insert all processes and their children.
		for (id, data) in items {
			let error_string = data.error.as_ref().map(|e| match e {
				Either::Left(data) => serde_json::to_string(data).unwrap(),
				Either::Right(id) => id.to_string(),
			});
			let error_code = data.error.as_ref().and_then(|e| match e {
				Either::Left(data) => data.code.map(|code| code.to_string()),
				Either::Right(_) => None,
			});
			let mounts_json =
				(!data.mounts.is_empty()).then(|| serde_json::to_string(&data.mounts).unwrap());
			let output_json = data
				.output
				.as_ref()
				.map(|o| serde_json::to_string(o).unwrap());

			let params = sqlite::params![
				id.to_string(),
				data.actual_checksum.as_ref().map(ToString::to_string),
				data.cacheable,
				data.command.to_string(),
				data.created_at,
				data.dequeued_at,
				data.enqueued_at,
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
				data.retry,
				data.started_at,
				data.status.to_string(),
				data.stderr.as_ref().map(ToString::to_string),
				data.stdin.as_ref().map(ToString::to_string),
				data.stdout.as_ref().map(ToString::to_string),
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
