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
		creator: Option<&tg::Principal>,
	) -> tg::Result<()> {
		self.put_process_batch_postgres(&[(id, &arg.data)], process_store, stored_at, creator)
			.await
	}

	pub(crate) async fn put_process_batch_postgres(
		&self,
		items: &[(&tg::process::Id, &tg::process::Data)],
		process_store: &db::postgres::Database,
		stored_at: i64,
		creator: Option<&tg::Principal>,
	) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}

		let items = items
			.iter()
			.map(|(id, data)| ((*id).clone(), (*data).clone()))
			.collect::<Vec<_>>();
		let creator = creator.cloned();

		process_store
			.run(|transaction| {
				let items = items.clone();
				let creator = creator.clone();
				async move {
					Self::put_process_batch_postgres_with_transaction(
						transaction,
						&items,
						stored_at,
						creator.as_ref(),
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
		creator: Option<&tg::Principal>,
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
		let mut stdins: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut stdouts: Vec<Option<String>> = Vec::with_capacity(items.len());
		let mut stored_ats = Vec::with_capacity(items.len());
		let mut creators = Vec::with_capacity(items.len());
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
			stderrs.push((!data.stderr.is_null()).then(|| data.stderr.to_string()));
			stdins.push((!data.stdin.is_null()).then(|| data.stdin.to_string()));
			stdouts.push((!data.stdout.is_null()).then(|| data.stdout.to_string()));
			stored_ats.push(stored_at);
			creators.push(creator.map(ToString::to_string));
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
					stdin,
					stdout,
					stored_at,
					creator,
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
					unnest($21::text[]),
					unnest($22::text[]),
					unnest($23::int8[]),
					unnest($24::text[]),
					unnest($25::text[])
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
					stdin = excluded.stdin,
					stdout = excluded.stdout,
					stored_at = excluded.stored_at,
					creator = excluded.creator,
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
					&stdins,
					&stdouts,
					&stored_ats,
					&creators,
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
		Ok(ControlFlow::Break(()))
	}
}
