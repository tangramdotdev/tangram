use crate::Server;
use bytes::Bytes;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn post_object(
		&self,
		arg: tg::object::post::Arg,
	) -> tg::Result<tg::object::post::Output> {
		let id = tg::object::Id::new(arg.kind, &arg.bytes);
		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Insert the object.
		#[derive(serde::Deserialize)]
		struct Row {
			complete: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, touched_at)
				values ({p}1, {p}2, {p}3)
				on conflict (id) do update set touched_at = {p}3
				returning complete;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![id, arg.bytes, now];
		let Row { complete } = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// If the object is not complete, then spawn a task to enqueue the object for indexing.
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
		let output = tg::object::post::Output { id: id.clone() };

		Ok(output)
	}
}

#[derive(serde::Deserialize)]
struct Body {
	kind: tg::object::Kind,
	bytes: Bytes,
}

impl Server {
	pub(crate) async fn handle_post_object_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let bytes = request.bytes().await?;
		let body: Body = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		let arg = tg::object::post::Arg {
			kind: body.kind,
			bytes: body.bytes,
		};
		let output = handle.post_object(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
