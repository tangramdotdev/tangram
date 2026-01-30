use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn put_process_postgres(
		id: &tg::process::Id,
		arg: &tg::process::put::Arg,
		database: &db::postgres::Database,
		touched_at: i64,
	) -> tg::Result<()> {
		Self::put_process_batch_postgres(&[(id, &arg.data)], database, touched_at).await
	}

	pub(crate) async fn put_process_batch_postgres(
		items: &[(&tg::process::Id, &tg::process::Data)],
		database: &db::postgres::Database,
		touched_at: i64,
	) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}

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

		// Collect all process data into column arrays.
		let mut ids = Vec::with_capacity(items.len());
		let mut actual_checksums: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut cacheables: Vec<bool> = Vec::with_capacity(items.len());
		let mut commands = Vec::with_capacity(items.len());
		let mut created_ats: Vec<i64> = Vec::with_capacity(items.len());
		let mut dequeued_ats: Vec<Option<i64>> = Vec::with_capacity(items.len());
		let mut enqueued_ats: Vec<Option<i64>> = Vec::with_capacity(items.len());
		let mut errors: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut error_codes: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut exits: Vec<Option<i64>> = Vec::with_capacity(items.len());
		let mut expected_checksums: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut finished_ats: Vec<Option<i64>> = Vec::with_capacity(items.len());
		let mut hosts: Vec<String> = Vec::with_capacity(items.len());
		let mut logs: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut mounts: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut networks: Vec<bool> = Vec::with_capacity(items.len());
		let mut outputs: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut retries: Vec<bool> = Vec::with_capacity(items.len());
		let mut started_ats: Vec<Option<i64>> = Vec::with_capacity(items.len());
		let mut statuses = Vec::with_capacity(items.len());
		let mut stderrs: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut stdins: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut stdouts: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut token_counts = Vec::with_capacity(items.len());
		let mut touched_ats = Vec::with_capacity(items.len());

		for (id, data) in items {
			ids.push(id.to_string());
			actual_checksums.push(data.actual_checksum.as_ref().map(ToString::to_string));
			cacheables.push(data.cacheable);
			commands.push(data.command.to_string());
			created_ats.push(data.created_at);
			dequeued_ats.push(data.dequeued_at);
			enqueued_ats.push(data.enqueued_at);
			errors.push(data.error.as_ref().map(|error| match error {
				tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
				tg::Either::Right(id) => id.to_string(),
			}));
			error_codes.push(
				data.error
					.as_ref()
					.and_then(|error| match error {
						tg::Either::Left(data) => data.code,
						tg::Either::Right(_) => None,
					})
					.as_ref()
					.map(ToString::to_string),
			);
			exits.push(data.exit.map(|exit| exit.to_i64().unwrap()));
			expected_checksums.push(data.expected_checksum.as_ref().map(ToString::to_string));
			finished_ats.push(data.finished_at);
			hosts.push(data.host.clone());
			logs.push(data.log.as_ref().map(ToString::to_string));
			mounts.push(
				(!data.mounts.is_empty()).then(|| serde_json::to_string(&data.mounts).unwrap()),
			);
			networks.push(data.network);
			outputs.push(
				data.output
					.as_ref()
					.map(|output| serde_json::to_string(output).unwrap()),
			);
			retries.push(data.retry);
			started_ats.push(data.started_at);
			statuses.push(data.status.to_string());
			stderrs.push(data.stderr.as_ref().map(ToString::to_string));
			stdins.push(data.stdin.as_ref().map(ToString::to_string));
			stdouts.push(data.stdout.as_ref().map(ToString::to_string));
			token_counts.push(0i64);
			touched_ats.push(touched_at);
		}

		// Insert all processes with UNNEST.
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
				select
					unnest($1::text[]),
					unnest($2::text[]),
					unnest($3::bool[]),
					unnest($4::text[]),
					unnest($5::int8[]),
					unnest($6::int8[]),
					unnest($7::int8[]),
					unnest($8::text[]),
					unnest($9::text[]),
					unnest($10::int8[]),
					unnest($11::text[]),
					unnest($12::int8[]),
					unnest($13::text[]),
					unnest($14::text[]),
					unnest($15::text[]),
					unnest($16::bool[]),
					unnest($17::text[]),
					unnest($18::bool[]),
					unnest($19::int8[]),
					unnest($20::text[]),
					unnest($21::text[]),
					unnest($22::text[]),
					unnest($23::text[]),
					unnest($24::int8[]),
					unnest($25::int8[])
				on conflict (id) do update set
					actual_checksum = excluded.actual_checksum,
					cacheable = excluded.cacheable,
					command = excluded.command,
					created_at = excluded.created_at,
					dequeued_at = excluded.dequeued_at,
					enqueued_at = excluded.enqueued_at,
					error = excluded.error,
					error_code = excluded.error_code,
					exit = excluded.exit,
					expected_checksum = excluded.expected_checksum,
					finished_at = excluded.finished_at,
					host = excluded.host,
					log = excluded.log,
					mounts = excluded.mounts,
					network = excluded.network,
					output = excluded.output,
					retry = excluded.retry,
					started_at = excluded.started_at,
					status = excluded.status,
					stderr = excluded.stderr,
					stdin = excluded.stdin,
					stdout = excluded.stdout,
					token_count = excluded.token_count,
					touched_at = excluded.touched_at;
			"
		);
		transaction
			.execute(
				statement,
				&[
					&ids,
					&actual_checksums,
					&cacheables,
					&commands,
					&created_ats,
					&dequeued_ats,
					&enqueued_ats,
					&errors,
					&error_codes,
					&exits,
					&expected_checksums,
					&finished_ats,
					&hosts,
					&logs,
					&mounts,
					&networks,
					&outputs,
					&retries,
					&started_ats,
					&statuses,
					&stderrs,
					&stdins,
					&stdouts,
					&token_counts,
					&touched_ats,
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Collect all children from all processes.
		let mut child_processes = Vec::new();
		let mut child_positions = Vec::new();
		let mut child_ids = Vec::new();
		let mut child_options = Vec::new();

		for (id, data) in items {
			if let Some(children) = &data.children {
				for (position, child) in children.iter().enumerate() {
					child_processes.push(id.to_string());
					child_positions.push(position.to_i64().unwrap());
					child_ids.push(child.item.to_string());
					child_options.push(serde_json::to_string(child.options()).unwrap());
				}
			}
		}

		// Insert all children with UNNEST.
		if !child_processes.is_empty() {
			let statement = indoc!(
				"
					insert into process_children (process, position, child, options)
					select unnest($1::text[]), unnest($2::int8[]), unnest($3::text[]), unnest($4::text[])
					on conflict (process, child) do nothing;
				"
			);
			transaction
				.execute(
					statement,
					&[
						&child_processes,
						&child_positions,
						&child_ids,
						&child_options,
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
