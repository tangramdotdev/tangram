use {
	crate::Session,
	futures::FutureExt as _,
	indoc::indoc,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn put_process_postgres(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::put::Arg,
		process_store: &db::postgres::Database,
		stored_at: i64,
		principal: Option<&tg::Principal>,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<()> {
		self.put_process_batch_postgres(
			&[(id, &arg.data)],
			process_store,
			stored_at,
			principal,
			created_by,
		)
		.await
	}

	pub(crate) async fn put_process_batch_postgres(
		&self,
		items: &[(&tg::process::Id, &tg::process::Data)],
		process_store: &db::postgres::Database,
		stored_at: i64,
		principal: Option<&tg::Principal>,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}

		let items = items
			.iter()
			.map(|(id, data)| ((*id).clone(), (*data).clone()))
			.collect::<Vec<_>>();
		let principal = principal.cloned();
		let created_by = created_by.cloned();
		let grant_ttl = self.server.config.process.grant_time_to_live;

		process_store
			.run(|transaction| {
				let items = items.clone();
				let principal = principal.clone();
				let created_by = created_by.clone();
				async move {
					Self::put_process_batch_postgres_with_transaction(
						transaction,
						&items,
						stored_at,
						principal.as_ref(),
						grant_ttl,
						created_by.as_ref(),
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put the process"))
	}

	pub(crate) async fn put_process_batch_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		items: &[(tg::process::Id, tg::process::Data)],
		stored_at: i64,
		principal: Option<&tg::Principal>,
		grant_ttl: std::time::Duration,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
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
		let mut lease_counts = Vec::with_capacity(items.len());
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
		let mut created_bys = Vec::with_capacity(items.len());
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
			lease_counts.push(0i64);
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
			created_bys.push(created_by.map(ToString::to_string));
			ttys.push(
				data.tty
					.as_ref()
					.map(|tty| serde_json::to_string(tty).unwrap()),
			);
		}

		let statement = indoc!(
			"
				insert into processes (
					actual_checksum,
					cacheable,
					command,
					created_at,
					debug,
					depth,
					error,
					error_code,
					exit,
					expected_checksum,
					finished_at,
					host,
					id,
					lease_count,
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
					created_by,
					tty
				)
				select
					unnest($1::text[]),
					unnest($2::bool[]),
					unnest($3::text[]),
					unnest($4::int8[]),
					unnest($5::text[]),
					null::int8,
					unnest($6::text[]),
					unnest($7::text[]),
					unnest($8::int8[]),
					unnest($9::text[]),
					unnest($10::int8[]),
					unnest($11::text[]),
					unnest($12::text[]),
					unnest($13::int8[]),
					unnest($14::text[]),
					unnest($15::text[]),
					unnest($16::bool[]),
					unnest($17::text[]),
					unnest($18::int8[]),
					unnest($19::text[]),
					unnest($20::text[]),
					unnest($21::bool[]),
					unnest($22::text[]),
					unnest($23::bool[]),
					unnest($24::text[]),
					unnest($25::bool[]),
					unnest($26::int8[]),
					unnest($27::text[]),
					unnest($28::text[])
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
					lease_count = excluded.lease_count,
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
					created_by = excluded.created_by,
					tty = excluded.tty;
			"
		);
		let result = transaction
			.inner()
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
					&lease_counts,
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
					&created_bys,
					&ttys,
				],
			)
			.await
			.map_err(db::postgres::Error::from);
		crate::database::retry!(result, "failed to execute the statement");

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

		if !child_processes.is_empty() {
			let statement = indoc!(
				"
					insert into process_children (process, position, cached, child, options)
					select unnest($1::text[]), unnest($2::int8[]), unnest($3::bool[]), unnest($4::text[]), unnest($5::text[])
					on conflict (process, child) do nothing;
				"
			);
			let result = transaction
				.inner()
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
				.map_err(db::postgres::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
		}

		if let Some(principal) = principal {
			let expires_at = stored_at + grant_ttl.as_secs().to_i64().unwrap();
			let processes = items
				.iter()
				.map(|(id, _)| id.to_string())
				.collect::<Vec<_>>();
			let principals = vec![principal.to_string(); processes.len()];
			let created_ats = vec![stored_at; processes.len()];
			let expires_ats = vec![expires_at; processes.len()];
			let statement = indoc!(
				"
					insert into process_grants (
						process,
						principal,
						node,
						node_command,
						node_error,
						node_log,
						node_output,
						subtree,
						subtree_command,
						subtree_error,
						subtree_log,
						subtree_output,
						created_at,
						expires_at
					)
					select
						unnest($1::text[]),
						unnest($2::text[]),
						true,
						true,
						true,
						true,
						true,
						true,
						true,
						true,
						true,
						true,
						unnest($3::int8[]),
						unnest($4::int8[])
					on conflict (process, principal) do update set
						node = process_grants.node or excluded.node,
						node_command = process_grants.node_command or excluded.node_command,
						node_error = process_grants.node_error or excluded.node_error,
						node_log = process_grants.node_log or excluded.node_log,
						node_output = process_grants.node_output or excluded.node_output,
						subtree = process_grants.subtree or excluded.subtree,
						subtree_command = process_grants.subtree_command or excluded.subtree_command,
						subtree_error = process_grants.subtree_error or excluded.subtree_error,
						subtree_log = process_grants.subtree_log or excluded.subtree_log,
						subtree_output = process_grants.subtree_output or excluded.subtree_output,
						expires_at = greatest(process_grants.expires_at, excluded.expires_at);
				"
			);
			let result = transaction
				.inner()
				.execute(
					statement,
					&[&processes, &principals, &created_ats, &expires_ats],
				)
				.await
				.map_err(db::postgres::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
		}

		Ok(ControlFlow::Break(()))
	}
}
