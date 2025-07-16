use crate::{Server, database::Database};
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use indoc::indoc;
#[cfg(feature = "postgres")]
use num::ToPrimitive as _;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::prelude::*;

impl Server {
	pub async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		// Insert the process into the database.
		match &self.database {
			Database::Sqlite(database) => {
				Self::put_process_sqlite(id, &arg, database, now).await?;
			},
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::put_process_postgres(id, &arg, database, now).await?;
			},
		}

		// Publish the process message.
		let output = arg
			.data
			.output
			.as_ref()
			.map(tg::value::data::Data::children)
			.into_iter()
			.flatten()
			.map(|output| (output, crate::index::ProcessObjectKind::Output));
		let command = std::iter::once((
			arg.data.command.clone().into(),
			crate::index::ProcessObjectKind::Command,
		));
		let objects = std::iter::empty().chain(output).chain(command).collect();
		let message = crate::index::Message::PutProcess(crate::index::PutProcessMessage {
			id: id.clone(),
			touched_at: now,
			children: arg.data.children.clone(),
			objects,
		});
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

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
			id,
			arg.data.actual_checksum,
			arg.data.cacheable,
			arg.data.command,
			arg.data.created_at,
			arg.data.dequeued_at,
			arg.data.enqueued_at,
			arg.data.error.as_ref().map(db::value::Json),
			arg.data.error.as_ref().and_then(|error| error.code),
			arg.data.exit,
			arg.data.expected_checksum,
			arg.data.finished_at,
			arg.data.host,
			arg.data.log,
			(!arg.data.mounts.is_empty()).then_some(db::value::Json(arg.data.mounts.clone())),
			arg.data.network,
			arg.data.output.as_ref().map(db::value::Json),
			arg.data.retry,
			arg.data.started_at,
			arg.data.status,
			arg.data.stderr,
			arg.data.stdin,
			arg.data.stdout,
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
					insert into process_children (process, position, child, path, tag)
					values (?1, ?2, ?3, ?4, ?5)
					on conflict (process, child) do nothing;
				"
			);
			children
				.iter()
				.enumerate()
				.map(|(position, child)| {
					let transaction = transaction.clone();
					async move {
						let params =
							db::params![id, position, child.item, child.path(), child.tag()];
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

	#[cfg(feature = "postgres")]
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
		if let Some(children) = &arg.data.children {
			if !children.is_empty() {
				let positions: Vec<i64> = (0..children.len().to_i64().unwrap()).collect();
				let statement = indoc!(
					"
						insert into process_children (process, position, child, path, tag)
						select $1, unnest($2::int8[]), unnest($3::text[]), unnest($4::text[]), unnest($5::text[])
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
								.map(|referent| {
									referent.path().map(|path| path.display().to_string())
								})
								.collect::<Vec<_>>(),
							&children
								.iter()
								.map(|referent| referent.tag().map(ToString::to_string))
								.collect::<Vec<_>>(),
						],
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
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

	pub(crate) async fn handle_put_process_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.put_process(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
