use {
	crate::Session,
	futures::FutureExt as _,
	indoc::indoc,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn put_process_turso(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::put::Arg,
		process_store: &db::turso::Database,
		stored_at: i64,
		creator: Option<&tg::Principal>,
	) -> tg::Result<()> {
		self.put_process_batch_turso(&[(id, &arg.data)], process_store, stored_at, creator)
			.await
	}

	pub(crate) async fn put_process_batch_turso(
		&self,
		items: &[(&tg::process::Id, &tg::process::Data)],
		process_store: &db::turso::Database,
		stored_at: i64,
		creator: Option<&tg::Principal>,
	) -> tg::Result<()> {
		if items.is_empty() {
			return Ok(());
		}

		let items: Vec<_> = items
			.iter()
			.map(|(id, data)| ((*id).clone(), (*data).clone()))
			.collect();
		let creator = creator.cloned();

		process_store
			.run(|transaction| {
				let items = items.clone();
				let creator = creator.clone();
				async move {
					Self::put_process_batch_turso_with_transaction(
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

	pub(crate) async fn put_process_batch_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		items: &[(tg::process::Id, tg::process::Data)],
		stored_at: i64,
		creator: Option<&tg::Principal>,
	) -> tg::Result<ControlFlow<(), db::turso::Error>> {
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
					?24
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
					log = ?13,
					output = ?14,
					retry = ?15,
					sandbox = ?16,
					started_at = ?17,
					status = ?18,
					stderr = ?19,
					stdin = ?20,
					stdout = ?21,
					stored_at = ?22,
					creator = ?23,
					tty = ?24
			"
		);
		let children_statement = indoc!(
			"
				insert into process_children (process, position, cached, child, options)
				values (?1, ?2, ?3, ?4, ?5)
				on conflict (process, child) do nothing;
			"
		);
		for (id, data) in items {
			let error_string = data.error.as_ref().map(|error| match error {
				tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
				tg::Either::Right(id) => id.clone().map_right(|id| id.id).into_inner().to_string(),
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

			let result = transaction
				.execute(
					process_statement.into(),
					db::params![
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
						data.log.as_ref().map(|log| log
							.clone()
							.map_right(|log| log.id)
							.into_inner()
							.to_string()),
						output_json,
						data.retry,
						data.sandbox.to_string(),
						data.started_at,
						data.status.to_string(),
						(!data.stderr.is_null()).then(|| data.stderr.to_string()),
						(!data.stdin.is_null()).then(|| data.stdin.to_string()),
						(!data.stdout.is_null()).then(|| data.stdout.to_string()),
						stored_at,
						creator.map(ToString::to_string),
						tty_json
					],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");

			if let Some(children) = &data.children {
				for (position, child) in children.iter().enumerate() {
					let result = transaction
						.execute(
							children_statement.into(),
							db::params![
								id.to_string(),
								position.to_i64().unwrap(),
								child.cached,
								child.process.item.to_string(),
								serde_json::to_string(&child.process.options).unwrap(),
							],
						)
						.await;
					crate::database::retry!(result, "failed to execute the statement");
				}
			}
		}

		Ok(ControlFlow::Break(()))
	}
}
