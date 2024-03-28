use crate::{params, Http, Server};
use bytes::Bytes;
use futures::TryFutureExt;
use http_body_util::BodyExt;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database as db;
use tangram_error::{error, Result};
use tangram_http::{bad_request, full, Incoming, Outgoing};

impl Server {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> Result<tg::object::PutOutput> {
		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

		// Add the object.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, children, complete, count, weight)
				values ({p}1, {p}2, 0, 0, null, null)
				on conflict (id) do update set id = excluded.id
				returning children;
			"
		);
		let params = params![id, arg.bytes];
		let children = connection
			.query_one_scalar_into(statement, params)
			.map_err(|error| error!(source = error, "failed to execute the statement"))
			.await?;

		// Find the incomplete children.
		let incomplete: Vec<tg::object::Id> = if children {
			let p = connection.p();
			let statement = formatdoc!(
				"
					select child
					from object_children
					left join objects on objects.id = object_children.child
					where object = {p}1 and complete = 0;
				"
			);
			let params = params![id];
			connection
				.query_all_scalar_into(statement, params)
				.map_err(|error| error!(source = error, "failed to execute the statement"))
				.await?
		} else {
			let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
				.map_err(|error| error!(source = error, "failed to deserialize the data"))?;
			data.children()
		};

		// Drop the database connection.
		drop(connection);

		let output = tg::object::PutOutput { incomplete };

		Ok(output)
	}

	pub(crate) async fn put_object_with_transaction(
		&self,
		id: tg::object::Id,
		bytes: Bytes,
		transaction: &db::Transaction<'_>,
	) -> Result<()> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, children, complete, count, weight)
				values ({p}1, {p}2, 0, 0, null, null)
				on conflict do nothing;
			"
		);
		let params = params![id, bytes];
		transaction
			.execute(statement, params)
			.map_err(|error| error!(source = error, "failed to execute the statement"))
			.await?;
		Ok(())
	}
}

impl Http {
	pub async fn handle_put_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|error| error!(source = error, "failed to read the body"))?
			.to_bytes();

		// Put the object.
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		let output = self.inner.tg.put_object(&id, &arg).await?;

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|error| error!(source = error, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
