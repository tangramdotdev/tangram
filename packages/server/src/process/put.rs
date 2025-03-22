use crate::Server;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use indoc::{formatdoc, indoc};
use itertools::Itertools as _;
use num::ToPrimitive;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc();

		// Send process message.
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
			touched_at: now.unix_timestamp(),
			children: arg.data.children.clone(),
			objects,
		});
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		self.messenger
			.publish("index".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		// Add to database.
		match &self.database {
			Either::Left(database) => {
				Self::put_process_sqlite(database, id, arg, &now.format(&Rfc3339).unwrap()).await?;
			},
			Either::Right(database) => {
				Self::put_process_postgres(database, id, arg, &now.format(&Rfc3339).unwrap())
					.await?;
			},
		};

		Ok(())
	}

	pub(crate) async fn put_process_sqlite(
		database: &db::sqlite::Database,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
		touched_at: &str,
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
					cacheable,
					checksum,
					command,
					created_at,
					cwd,
					dequeued_at,
					enqueued_at,
					env,
					error,
					exit,
					finished_at,
					host,
					log,
					network,
					output,
					retry,
					started_at,
					status,
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
					?20
				)
				on conflict (id) do update set
					cacheable = ?2,
					checksum = ?3,
					command = ?4,
					created_at = ?5,
					cwd = ?6,
					dequeued_at = ?7,
					enqueued_at = ?8,
					env = ?9,
					error = ?10,
					exit = ?11,
					finished_at = ?12,
					host = ?13,
					log = ?14,
					network = ?15,
					output = ?16,
					retry = ?17,
					started_at = ?18,
					status = ?19,
					touched_at = ?20;
			"
		);
		let params = db::params![
			id,
			arg.data.cacheable,
			arg.data.checksum,
			arg.data.command,
			arg.data.created_at.format(&Rfc3339).unwrap(),
			arg.data.cwd,
			arg.data.dequeued_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.data.enqueued_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.data.env.as_ref().map(db::value::Json),
			arg.data.error.as_ref().map(db::value::Json),
			arg.data.exit.as_ref().map(db::value::Json),
			arg.data.finished_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.data.host,
			arg.data.log,
			arg.data.network,
			arg.data.output.as_ref().map(db::value::Json),
			arg.data.retry,
			arg.data.started_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.data.status,
			touched_at
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the children.
		if let Some(children) = &arg.data.children {
			let statement = formatdoc!(
				"
					insert into process_children (process, position, child)
					values (?1, ?2, ?3);
			"
			);
			children
				.iter()
				.enumerate()
				.map(|(position, child)| {
					let transaction = transaction.clone();
					let statement = statement.clone();
					async move {
						let params = db::params![id, position, child];
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

		// Insert the objects.
		let statement = formatdoc!(
			"
				insert into process_objects (process, object)
				values (?1, ?2)
				on conflict (process, object) do nothing;
			"
		);
		let objects = arg
			.data
			.log
			.into_iter()
			.map_into()
			.chain(
				arg.data
					.output
					.as_ref()
					.map(tg::value::Data::children)
					.into_iter()
					.flatten(),
			)
			.chain(std::iter::once(arg.data.command.clone().into()));
		objects
			.map(|object| {
				let transaction = transaction.clone();
				let statement = statement.clone();
				async move {
					let params = db::params![id, object];
					transaction
						.execute(statement.into(), params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

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

	pub(crate) async fn put_process_postgres(
		database: &db::postgres::Database,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
		touched_at: &str,
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
					cacheable,
					checksum,
					command,
					created_at,
					cwd,
					dequeued_at,
					enqueued_at,
					env,
					error,
					exit,
					finished_at,
					host,
					log,
					network,
					output,
					retry,
					started_at,
					status,
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
					$20
				)
				on conflict (id) do update set
					cacheable = $2,
					checksum = $3,
					command = $4,
					created_at = $5,
					cwd = $6,
					dequeued_at = $7,
					enqueued_at = $8,
					env = $9,
					error = $10,
					exit = $11,
					finished_at = $12,
					host = $13,
					log = $14,
					network = $15,
					output = $16,
					retry = $17,
					started_at = $18,
					status = $19,
					touched_at = $20;
			"
		);
		transaction
			.execute(
				statement,
				&[
					&id.to_string(),
					&i64::from(arg.data.cacheable),
					&arg.data.checksum.map(|checksum| checksum.to_string()),
					&arg.data.command.to_string(),
					&arg.data.created_at.format(&Rfc3339).unwrap(),
					&arg.data.cwd.map(|cwd| cwd.to_string_lossy().to_string()),
					&arg.data.dequeued_at.map(|t| t.format(&Rfc3339).unwrap()),
					&arg.data.enqueued_at.map(|t| t.format(&Rfc3339).unwrap()),
					&serde_json::to_string(&arg.data.env.as_ref()).unwrap(),
					&serde_json::to_string(&arg.data.error.as_ref()).unwrap(),
					&serde_json::to_string(&arg.data.exit.as_ref()).unwrap(),
					&arg.data.finished_at.map(|t| t.format(&Rfc3339).unwrap()),
					&arg.data.host,
					&arg.data
						.log
						.as_ref()
						.map(|log| serde_json::to_string(&log).unwrap()),
					&i64::from(arg.data.network),
					&serde_json::to_string(&arg.data.output.as_ref()).unwrap(),
					&i64::from(arg.data.retry),
					&arg.data.started_at.map(|t| t.format(&Rfc3339).unwrap()),
					&serde_json::to_string(&arg.data.status).unwrap(),
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
						insert into process_children (process, position, child)
						select $1, unnest($2::int8[]), unnest($3::text[]);
					"
				);
				transaction
					.execute(
						statement,
						&[
							&id.to_string(),
							&positions.as_slice(),
							&children.iter().map(ToString::to_string).collect::<Vec<_>>(),
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
}

impl Server {
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
