use {
	crate::Session,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn put_process_sqlite(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::put::Arg,
		process_store: &db::sqlite::Database,
		stored_at: i64,
		creator: Option<&tg::Principal>,
	) -> tg::Result<()> {
		self.put_process_batch_sqlite(&[(id, &arg.data)], process_store, stored_at, creator)
			.await
	}

	pub(crate) async fn put_process_batch_sqlite(
		&self,
		items: &[(&tg::process::Id, &tg::process::Data)],
		process_store: &db::sqlite::Database,
		stored_at: i64,
		creator: Option<&tg::Principal>,
	) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}

		// Clone items for the closure.
		let items: Vec<_> = items
			.iter()
			.map(|(id, data)| ((*id).clone(), (*data).clone()))
			.collect();
		let creator = creator.cloned();

		process_store
			.run(move |transaction, cache| {
				Self::put_process_batch_sqlite_sync(
					transaction,
					cache,
					&items,
					stored_at,
					creator.as_ref(),
				)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put the process"))
	}

	pub(crate) fn put_process_batch_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		items: &[(tg::process::Id, tg::process::Data)],
		stored_at: i64,
		creator: Option<&tg::Principal>,
	) -> tg::Result<ControlFlow<(), db::sqlite::Error>> {
		let process_statement = indoc!(
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
					creator,
					tty
				)
				values (
					?1,
					?2,
					?3,
					?4,
					?5,
					null,
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
					?26,
					?27,
					?28
				)
				on conflict (id) do update set
					actual_checksum = ?1,
					cacheable = ?2,
					command = ?3,
					created_at = ?4,
					debug = ?5,
					error = ?6,
					error_code = ?7,
					exit = ?8,
					expected_checksum = ?9,
					finished_at = ?10,
					host = ?11,
					lease_count = ?13,
					log = ?14,
					output = ?15,
					retry = ?16,
					sandbox = ?17,
					started_at = ?18,
					status = ?19,
					stderr = ?20,
					stderr_open = ?21,
					stdin = ?22,
					stdin_open = ?23,
					stdout = ?24,
					stdout_open = ?25,
					stored_at = ?26,
					creator = ?27,
					tty = ?28
			"
		);
		let result = cache.get(transaction, process_statement.into());
		let mut process_statement =
			crate::database::retry!(result, "failed to prepare the statement");

		let children_statement = indoc!(
			"
				insert into process_children (process, position, cached, child, options)
				values (?1, ?2, ?3, ?4, ?5)
				on conflict (process, child) do nothing;
			"
		);
		let result = cache.get(transaction, children_statement.into());
		let mut children_statement =
			crate::database::retry!(result, "failed to prepare the statement");

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
			let debug_json = data
				.debug
				.as_ref()
				.map(|debug| serde_json::to_string(debug).unwrap());
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
				data.actual_checksum.as_ref().map(ToString::to_string),
				data.cacheable,
				data.command.to_string(),
				data.created_at,
				debug_json,
				error_string,
				error_code,
				data.exit,
				data.expected_checksum.as_ref().map(ToString::to_string),
				data.finished_at,
				data.host,
				id.to_string(),
				0,
				data.log.as_ref().map(ToString::to_string),
				output_json,
				data.retry,
				data.sandbox.to_string(),
				data.started_at,
				data.status.to_string(),
				(!data.stderr.is_null()).then(|| data.stderr.to_string()),
				stderr_open,
				(!data.stdin.is_null()).then(|| data.stdin.to_string()),
				stdin_open,
				(!data.stdout.is_null()).then(|| data.stdout.to_string()),
				stdout_open,
				stored_at,
				creator.map(ToString::to_string),
				tty_json
			];
			let result = process_statement
				.execute(params)
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement");

			if let Some(children) = &data.children {
				for (position, child) in children.iter().enumerate() {
					let params = sqlite::params![
						id.to_string(),
						position.to_i64().unwrap(),
						child.cached,
						child.process.to_string(),
						serde_json::to_string(&child.options).unwrap(),
					];
					let result = children_statement
						.execute(params)
						.map_err(db::sqlite::Error::from);
					crate::database::retry!(result, "failed to execute the statement");
				}
			}
		}

		drop(process_statement);
		drop(children_statement);

		Ok(ControlFlow::Break(()))
	}
}
