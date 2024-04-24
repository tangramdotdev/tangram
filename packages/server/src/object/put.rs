use crate::{
	database::Transaction,
	util::http::{bad_request, full, Incoming, Outgoing},
	Http, Server,
};
use futures::{FutureExt as _, TryFutureExt as _};
use http_body_util::BodyExt as _;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::PutArg,
		transaction: Option<&Transaction<'_>>,
	) -> tg::Result<tg::object::PutOutput> {
		if let Some(transaction) = transaction {
			self.put_object_with_transaction(id, arg, transaction).await
		} else {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// // Begin a transaction.
			// let transaction = connection
			// 	.transaction()
			// 	.boxed()
			// 	.await
			// 	.map_err(|source| tg::error!(!source, "failed to begin the transaction"))?;

			// Put the object.
			let output = self
				.put_object_with_transaction(id, arg, &connection)
				.await?;

			// // Commit the transaction.
			// transaction
			// 	.commit()
			// 	.await
			// 	.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

			// Drop the connection.
			drop(connection);

			Ok(output)
		}
	}

	pub(crate) async fn put_object_with_transaction(
		&self,
		id: &tg::object::Id,
		arg: tg::object::PutArg,
		transaction: &impl db::Query,
	) -> tg::Result<tg::object::PutOutput> {
		// Add the object.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, indexed, complete, count, weight, touched_at)
				values ({p}1, {p}2, 0, 0, null, null, {p}3)
				on conflict (id) do update set touched_at = {p}3
				returning indexed;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![id, arg.bytes, now];
		let indexed = transaction
			.query_one_value_into(statement, params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))
			.await?;

		// Get the incomplete children.
		let incomplete: Vec<tg::object::Id> = if indexed {
			// If the object is indexed, then use the object_children table.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					select child
					from object_children
					left join objects on objects.id = object_children.child
					where object = {p}1 and complete = 0;
				"
			);
			let params = db::params![id];
			transaction
				.query_all_value_into(statement, params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))
				.await?
		} else {
			// If the object is not indexed, then return all the children.
			let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
			data.children()
		};

		let output = tg::object::PutOutput { incomplete };

		Ok(output)
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_put_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();

		// Put the object.
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		let output = self.handle.put_object(&id, arg, None).boxed().await?;

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
