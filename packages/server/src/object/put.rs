use crate::Server;
use indoc::formatdoc;
use std::collections::BTreeSet;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Insert the object.
		#[derive(serde::Deserialize)]
		struct Row {
			children: bool,
			complete: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, touched_at)
				values ({p}1, {p}2, {p}3)
				on conflict (id) do update set touched_at = {p}3
				returning children, complete;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![id, arg.bytes, now];
		let Row { children, complete } = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Get the incomplete children.
		let incomplete: BTreeSet<tg::object::Id> = if children {
			// If the object's children are set, then get the incomplete children.
			let p = connection.p();
			let statement = formatdoc!(
				"
					select child
					from object_children
					left join objects on objects.id = object_children.child
					where object_children.object = {p}1 and objects.complete = 0;
				"
			);
			let params = db::params![id];
			connection
				.query_all_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
				.into_iter()
				.collect()
		} else {
			// If the children are not set, then return all the children.
			let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
			data.children()
		};

		// Drop the connection.
		drop(connection);

		// If the object is not complete and has no incomplete children, then spawn a task to enqueue the object for indexing.
		if !complete {
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				async move {
					server
						.enqueue_objects_for_indexing(&[id])
						.await
						.inspect_err(|error| tracing::error!(?error))
						.ok();
				}
			});
		}

		// Create the output.
		let output = tg::object::put::Output { incomplete };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_put_object_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let bytes = request.bytes().await?;
		let arg = tg::object::put::Arg { bytes };
		let output = handle.put_object(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
