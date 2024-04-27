use crate::{
	util::http::{bad_request, full, Incoming, Outgoing},
	Server,
};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

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

		// Get the builds.
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
					count,
					host,
					log,
					outcome,
					retry,
					status,
					target,
					weight,
					created_at,
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
		let Some(query) = request.uri().query() else {
			return Ok(bad_request());
		};
		let arg = serde_urlencoded::from_str(query)
			.map_err(|source| tg::error!(!source, "failed to deserialize the query"))?;

		let output = handle.list_builds(arg).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
