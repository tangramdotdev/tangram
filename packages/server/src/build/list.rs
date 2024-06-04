use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn list_builds(
		&self,
		arg: tg::build::list::Arg,
	) -> tg::Result<tg::build::list::Output> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// List the builds.
		let status = if arg.status.is_some() {
			"false"
		} else {
			"true"
		};
		let target = if arg.target.is_some() {
			"false"
		} else {
			"true"
		};
		let order = match arg.order.unwrap_or(tg::build::list::Order::CreatedAt) {
			tg::build::list::Order::CreatedAt => "order by created_at",
			tg::build::list::Order::CreatedAtDesc => "order by created_at desc",
		};
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
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
					finished_at
				from builds
				where
					(status = {p}1 or {status}) and
					(target = {p}2 or {target})
				{order}
				limit {p}3
				offset {p}4;
			"
		);
		let params = db::params![arg.status, arg.target, arg.limit, 0];
		let items = connection
			.query_all_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::build::list::Output { items };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_list_builds_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_builds(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
