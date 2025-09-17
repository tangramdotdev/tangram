use crate::Server;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use indoc::indoc;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub(crate) async fn put_process_sqlite(
		id: &tg::process::Id,
		arg: &tg::process::put::Arg,
		database: &db::sqlite::Database,
		touched_at: i64,
	) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let transaction = Arc::new(transaction);

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
		let params = db::params![
			id.to_bytes(),
			arg.data.actual_checksum.as_ref().map(ToString::to_string),
			arg.data.cacheable,
			arg.data.command.to_bytes(),
			arg.data.created_at,
			arg.data.dequeued_at,
			arg.data.enqueued_at,
			arg.data.error.as_ref().map(db::value::Json),
			arg.data
				.error
				.as_ref()
				.and_then(|error| error.code.map(|code| code.to_string())),
			arg.data.exit,
			arg.data.expected_checksum.as_ref().map(ToString::to_string),
			arg.data.finished_at,
			arg.data.host,
			arg.data.log.as_ref().map(|log| log.to_bytes()),
			(!arg.data.mounts.is_empty()).then_some(db::value::Json(arg.data.mounts.clone())),
			arg.data.network,
			arg.data.output.as_ref().map(db::value::Json),
			arg.data.retry,
			arg.data.started_at,
			arg.data.status.to_string(),
			arg.data.stderr.as_ref().map(ToString::to_string),
			arg.data.stdin.as_ref().map(ToString::to_string),
			arg.data.stdout.as_ref().map(ToString::to_string),
			0,
			touched_at
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the children.
		if let Some(children) = &arg.data.children {
			let statement = indoc!(
				"
					insert into process_children (process, position, child, options)
					values (?1, ?2, ?3, ?4)
					on conflict (process, child) do nothing;
				"
			);
			children
				.iter()
				.enumerate()
				.map(|(position, child)| {
					let transaction = transaction.clone();
					async move {
						let params = db::params![
							id.to_bytes(),
							position,
							child.item.to_bytes(),
							serde_json::to_string(child.options()).unwrap(),
						];
						transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						Ok::<_, tg::Error>(())
					}
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<()>()
				.await?;
		}
		// Commit the transaction.
		Arc::into_inner(transaction)
			.unwrap()
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

		Ok(())
	}
}
