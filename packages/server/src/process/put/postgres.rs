use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn put_process_postgres(
		id: &tg::process::Id,
		arg: &tg::process::put::Arg,
		database: &db::postgres::Database,
		touched_at: i64,
	) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.inner_mut()
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Insert the process.
		let statement = indoc!(
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
					$1,
					$2,
					$3,
					$4,
					$5,
					$6,
					$7,
					$8,
					$9,
					$10,
					$11,
					$12,
					$13,
					$14,
					$15,
					$16,
					$17,
					$18,
					$19,
					$20,
					$21,
					$22,
					$23,
					$24,
					$25
				)
				on conflict (id) do update set
					actual_checksum = $2,
					cacheable = $3,
					command = $4,
					created_at = $5,
					dequeued_at = $6,
					enqueued_at = $7,
					error = $8,
					error_code = $9,
					exit = $10,
					expected_checksum = $11,
					finished_at = $12,
					host = $13,
					log = $14,
					mounts = $15,
					network = $16,
					output = $17,
					retry = $18,
					started_at = $19,
					status = $20,
					stderr = $21,
					stdin = $22,
					stdout = $23,
					token_count = $24,
					touched_at = $25;
			"
		);
		transaction
			.execute(
				statement,
				&[
					&id.to_string(),
					&arg.data.actual_checksum.as_ref().map(ToString::to_string),
					&i64::from(arg.data.cacheable),
					&arg.data.command.to_string(),
					&arg.data.created_at,
					&arg.data.dequeued_at,
					&arg.data.enqueued_at,
					&arg.data
						.error
						.as_ref()
						.map(|error| serde_json::to_string(error).unwrap()),
					&arg.data
						.error
						.as_ref()
						.and_then(|error| error.code)
						.as_ref()
						.map(ToString::to_string),
					&arg.data.exit.map(|exit| exit.to_i64().unwrap()),
					&arg.data.expected_checksum.as_ref().map(ToString::to_string),
					&arg.data.finished_at,
					&arg.data.host,
					&arg.data
						.log
						.as_ref()
						.map(|log| serde_json::to_string(&log).unwrap()),
					&(!arg.data.mounts.is_empty())
						.then(|| serde_json::to_string(&arg.data.mounts).unwrap()),
					&i64::from(arg.data.network),
					&arg.data
						.output
						.as_ref()
						.map(|error| serde_json::to_string(error).unwrap()),
					&i64::from(arg.data.retry),
					&arg.data.started_at,
					&arg.data.status.to_string(),
					&arg.data.stderr.as_ref().map(ToString::to_string),
					&arg.data.stdin.as_ref().map(ToString::to_string),
					&arg.data.stdout.as_ref().map(ToString::to_string),
					&0i64,
					&touched_at,
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the children.
		if let Some(children) = &arg.data.children
			&& !children.is_empty()
		{
			let positions: Vec<i64> = (0..children.len().to_i64().unwrap()).collect();
			let statement = indoc!(
				"
						insert into process_children (process, position, child, options)
						select $1, unnest($2::int8[]), unnest($3::text[]), unnest($4::text[])
						on conflict (process, child) do nothing;
					"
			);
			transaction
				.execute(
					statement,
					&[
						&id.to_string(),
						&positions.as_slice(),
						&children
							.iter()
							.map(|referent| referent.item.to_string())
							.collect::<Vec<_>>(),
						&children
							.iter()
							.map(|referent| serde_json::to_string(referent.options()).unwrap())
							.collect::<Vec<_>>(),
					],
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		Ok(())
	}
}
