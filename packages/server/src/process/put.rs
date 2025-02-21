use crate::Server;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use indoc::formatdoc;
use itertools::Itertools as _;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
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
		let p = transaction.p();
		let statement = formatdoc!(
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
					{p}15,
					{p}16,
					{p}17,
					{p}18,
					{p}19,
					{p}20
				)
				on conflict (id) do update set
					cacheable = {p}2,
					checksum = {p}3,
					command = {p}4,
					created_at = {p}5,
					cwd = {p}6,
					dequeued_at = {p}7,
					enqueued_at = {p}8,
					env = {p}9,
					error = {p}10,
					exit = {p}11,
					finished_at = {p}12,
					host = {p}13,
					log = {p}14,
					network = {p}15,
					output = {p}16,
					retry = {p}17,
					started_at = {p}18,
					status = {p}19,
					touched_at = {p}20;
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
			time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
		];
		transaction
			.execute(statement.into(), params)
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
		if let Some(children) = &arg.data.children {
			let p = transaction.p();
			let statement = formatdoc!(
				"
				insert into process_children (process, position, child)
				values ({p}1, {p}2, {p}3);
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
