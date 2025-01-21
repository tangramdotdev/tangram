use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<tg::process::put::Output> {
		// Get a database connection.
		let mut connection = self
			.database
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
		#[allow(clippy::struct_excessive_bools)]
		#[derive(serde::Deserialize)]
		struct Row {
			complete: bool,
			logs_complete: bool,
			outputs_complete: bool,
			targets_complete: bool,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into processes (
					id,
					depth,
					error,
					host,
					log,
					output,
					retry,
					status,
					target,
					touched_at,
					created_at,
					enqueued_at,
					dequeued_at,
					started_at,
					finished_at
				)
				values (
					{p}1,
					{p}2,
					{p}3,
					{p}4,
					{p}5,
					{p}6,
					{p}7,
					{p}8,
					{p}9,
					{p}10,
					{p}11,
					{p}12,
					{p}13,
					{p}14,
					{p}15
				)
				on conflict (id) do update set
					depth = {p}2,
					error = {p}3,
					host = {p}4,
					log = {p}5,
					output = {p}6,
					retry = {p}7,
					status = {p}8,
					command = {p}9,
					touched_at = {p}10,
					created_at = {p}11,
					enqueued_at = {p}12,
					dequeued_at = {p}13,
					started_at = {p}14,
					finished_at = {p}15
				returning
					complete,
					logs_complete,
					outputs_complete,
					targets_complete;
			"
		);
		let params = db::params![
			id,
			arg.depth,
			arg.error,
			arg.host,
			arg.logs,
			arg.output.as_ref().map(db::value::Json),
			arg.retry,
			arg.status,
			arg.command,
			time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
			arg.created_at.format(&Rfc3339).unwrap(),
			arg.enqueued_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.dequeued_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.started_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.finished_at.map(|t| t.format(&Rfc3339).unwrap()),
		];
		let row = transaction
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Delete any existing children.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from process_children
				where process = {p}1;
			"
		);
		let params = db::params![id];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the children.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into process_children (process, position, child)
				values ({p}1, {p}2, {p}3);
			"
		);
		arg.children
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
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;

		// Delete any existing objects.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from process_objects
				where process = {p}1;
			"
		);
		let params = db::params![id];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the objects.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into process_objects (process, object)
				values ({p}1, {p}2)
				on conflict (process, object) do nothing;
			"
		);
		let objects = arg
			.logs
			.into_iter()
			.flat_map(|objects| objects.into_iter().map(|object| object.unwrap_object()))
			.into_iter()
			.chain(
				arg.output
					.as_ref()
					.map(tg::value::Data::children)
					.into_iter()
					.flatten(),
			)
			.chain(std::iter::once(arg.command.clone().into()));
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

		// Create the output.
		let output = tg::process::put::Output {
			complete: row.complete,
			logs_complete: row.logs_complete,
			outputs_complete: row.outputs_complete,
			targets_complete: row.targets_complete,
		};

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_put_process_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.put_process(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
