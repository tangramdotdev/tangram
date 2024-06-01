use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_build(&self, id: &tg::build::Id, arg: tg::build::put::Arg) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let transaction = Arc::new(transaction);

		// Delete any existing children.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from build_children
				where build = {p}1;
			"
		);
		let params = db::params![id];
		transaction
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Insert the children.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into build_children (build, position, child)
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
						.execute(statement, params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Delete any existing objects.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from build_objects
				where build = {p}1;
			"
		);
		let params = db::params![id];
		transaction
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Add the objects.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into build_objects (build, object)
				values ({p}1, {p}2);
			"
		);
		let objects = arg
			.log
			.clone()
			.map(Into::into)
			.into_iter()
			.chain(
				arg.outcome
					.as_ref()
					.and_then(|outcome| outcome.try_unwrap_succeeded_ref().ok())
					.map(tg::value::Data::children)
					.into_iter()
					.flatten(),
			)
			.chain(std::iter::once(arg.target.clone().into()));
		objects
			.map(|object| {
				let transaction = transaction.clone();
				let statement = statement.clone();
				async move {
					let params = db::params![id, object];
					transaction
						.execute(statement, params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Insert the build.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into builds (
					id,
					host,
					log,
					outcome,
					retry,
					status,
					target,
					created_at,
					dequeued_at,
					started_at,
					finished_at,
					heartbeat_at,
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
					{p}13
				)
				on conflict (id) do update set
					host = {p}2,
					log = {p}3,
					outcome = {p}4,
					retry = {p}5,
					status = {p}6,
					target = {p}7,
					created_at = {p}8,
					dequeued_at = {p}9,
					started_at = {p}10,
					finished_at = {p}11,
					heartbeat_at = {p}12,
					touched_at = {p}13;
			"
		);
		let params = db::params![
			id,
			arg.host,
			arg.log,
			arg.outcome,
			arg.retry,
			arg.status,
			arg.target,
			arg.created_at.format(&Rfc3339).unwrap(),
			arg.dequeued_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.started_at.map(|t| t.format(&Rfc3339).unwrap()),
			arg.finished_at.map(|t| t.format(&Rfc3339).unwrap()),
			db::Value::Null,
			time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
		];
		transaction
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Commit the transaction.
		Arc::into_inner(transaction)
			.unwrap()
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_put_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.put_build(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
