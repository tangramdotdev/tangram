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
		process_store: &db::postgres::Database,
		stored_at: i64,
	) -> tg::Result<()> {
		Self::put_process_batch_postgres(&[(id, &arg.data)], process_store, stored_at).await
	}

	pub(crate) async fn put_process_batch_postgres(
		items: &[(&tg::process::Id, &tg::process::Data)],
		process_store: &db::postgres::Database,
		stored_at: i64,
	) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}

		// Get a process store connection.
		let mut connection = process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;

		// Begin a transaction.
		let transaction = connection
			.inner_mut()
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Collect all process data into column arrays.
		let mut actual_checksums: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut cacheables: Vec<bool> = Vec::with_capacity(items.len());
		let mut commands = Vec::with_capacity(items.len());
		let mut created_ats: Vec<i64> = Vec::with_capacity(items.len());
		let mut debugs: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut errors: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut error_codes: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut exits: Vec<Option<i64>> = Vec::with_capacity(items.len());
		let mut expected_checksums: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut finished_ats: Vec<Option<i64>> = Vec::with_capacity(items.len());
		let mut hosts: Vec<String> = Vec::with_capacity(items.len());
		let mut ids = Vec::with_capacity(items.len());
		let mut logs: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut outputs: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut retries: Vec<bool> = Vec::with_capacity(items.len());
		let mut sandboxes: Vec<String> = Vec::with_capacity(items.len());
		let mut started_ats: Vec<Option<i64>> = Vec::with_capacity(items.len());
		let mut statuses = Vec::with_capacity(items.len());
		let mut stderrs: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut stderr_opens: Vec<Option<bool>> = Vec::with_capacity(items.len());
		let mut stdins: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut stdin_opens: Vec<Option<bool>> = Vec::with_capacity(items.len());
		let mut stdouts: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut stdout_opens: Vec<Option<bool>> = Vec::with_capacity(items.len());
		let mut stored_ats = Vec::with_capacity(items.len());
		let mut token_counts = Vec::with_capacity(items.len());
		let mut ttys: Vec<Option<String>> = Vec::with_capacity(items.len());

		for (id, data) in items {
			actual_checksums.push(data.actual_checksum.as_ref().map(ToString::to_string));
			cacheables.push(data.cacheable);
			commands.push(data.command.to_string());
			created_ats.push(data.created_at);
			debugs.push(
				data.debug
					.as_ref()
					.map(|debug| serde_json::to_string(debug).unwrap()),
			);
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
			ids.push(id.to_string());
			logs.push(data.log.as_ref().map(ToString::to_string));
			outputs.push(
				data.output
					.as_ref()
					.map(|output| serde_json::to_string(output).unwrap()),
			);
			retries.push(data.retry);
			sandboxes.push(data.sandbox.to_string());
			started_ats.push(data.started_at);
			statuses.push(data.status.to_string());
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
			stderrs.push((!data.stderr.is_null()).then(|| data.stderr.to_string()));
			stderr_opens.push(stderr_open);
			stdins.push((!data.stdin.is_null()).then(|| data.stdin.to_string()));
			stdin_opens.push(stdin_open);
			stdouts.push((!data.stdout.is_null()).then(|| data.stdout.to_string()));
			stdout_opens.push(stdout_open);
			stored_ats.push(stored_at);
			token_counts.push(0i64);
			ttys.push(
				data.tty
					.as_ref()
					.map(|tty| serde_json::to_string(tty).unwrap()),
			);
		}

		// Insert all processes with UNNEST.
		let statement = indoc!(
			"
				insert into processes (
					actual_checksum,
					cacheable,
					command,
					created_at,
					debug,
					error,
					error_code,
					exit,
					expected_checksum,
					finished_at,
					host,
					id,
					log,
					output,
					retry,
					sandbox,
					started_at,
					status,
					stderr,
					stderr_open,
					stdin,
					stdin_open,
					stdout,
					stdout_open,
					stored_at,
					token_count,
					tty
				)
				select
					unnest($1::text[]),
					unnest($2::bool[]),
					unnest($3::text[]),
					unnest($4::int8[]),
					unnest($5::text[]),
					unnest($6::text[]),
					unnest($7::text[]),
					unnest($8::int8[]),
					unnest($9::text[]),
					unnest($10::int8[]),
					unnest($11::text[]),
					unnest($12::text[]),
					unnest($13::text[]),
					unnest($14::text[]),
					unnest($15::bool[]),
					unnest($16::text[]),
					unnest($17::int8[]),
					unnest($18::text[]),
					unnest($19::text[]),
					unnest($20::bool[]),
					unnest($21::text[]),
					unnest($22::bool[]),
					unnest($23::text[]),
					unnest($24::bool[]),
					unnest($25::int8[]),
					unnest($26::int8[]),
					unnest($27::text[])
				on conflict (id) do update set
					actual_checksum = excluded.actual_checksum,
					cacheable = excluded.cacheable,
					command = excluded.command,
					created_at = excluded.created_at,
					debug = excluded.debug,
					error = excluded.error,
					error_code = excluded.error_code,
					exit = excluded.exit,
					expected_checksum = excluded.expected_checksum,
					finished_at = excluded.finished_at,
					host = excluded.host,
					log = excluded.log,
					output = excluded.output,
					retry = excluded.retry,
					sandbox = excluded.sandbox,
					started_at = excluded.started_at,
					status = excluded.status,
					stderr = excluded.stderr,
					stderr_open = excluded.stderr_open,
					stdin = excluded.stdin,
					stdin_open = excluded.stdin_open,
					stdout = excluded.stdout,
					stdout_open = excluded.stdout_open,
					stored_at = excluded.stored_at,
					token_count = excluded.token_count,
					tty = excluded.tty;
			"
		);
		transaction
			.execute(
				statement,
				&[
					&actual_checksums,
					&cacheables,
					&commands,
					&created_ats,
					&debugs,
					&errors,
					&error_codes,
					&exits,
					&expected_checksums,
					&finished_ats,
					&hosts,
					&ids,
					&logs,
					&outputs,
					&retries,
					&sandboxes,
					&started_ats,
					&statuses,
					&stderrs,
					&stderr_opens,
					&stdins,
					&stdin_opens,
					&stdouts,
					&stdout_opens,
					&stored_ats,
					&token_counts,
					&ttys,
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Collect all children from all processes.
		let mut child_processes = Vec::new();
		let mut child_positions = Vec::new();
		let mut child_cached = Vec::new();
		let mut child_ids = Vec::new();
		let mut child_options = Vec::new();

		for (id, data) in items {
			if let Some(children) = &data.children {
				for (position, child) in children.iter().enumerate() {
					child_processes.push(id.to_string());
					child_positions.push(position.to_i64().unwrap());
					child_cached.push(child.cached);
					child_ids.push(child.process.to_string());
					child_options.push(serde_json::to_string(&child.options).unwrap());
				}
			}
		}

		// Insert all children with UNNEST.
		if !child_processes.is_empty() {
			let statement = indoc!(
				"
					insert into process_children (process, position, cached, child, options)
					select unnest($1::text[]), unnest($2::int8[]), unnest($3::bool[]), unnest($4::text[]), unnest($5::text[])
					on conflict (process, child) do nothing;
				"
			);
			transaction
				.execute(
					statement,
					&[
						&child_processes,
						&child_positions,
						&child_cached,
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
