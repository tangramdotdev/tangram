use crate::Server;
use indoc::formatdoc;
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
		// If there is a store, then put the object in the store.
		if let Some(store) = &self.store {
			store
				.put(id.clone(), arg.bytes.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to put the object in the store"))?;
		} else {
			// Get a database connection.
			let connection = self
				.database
				.write_connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Insert the object.
			let p = connection.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, touched_at)
					values ({p}1, {p}2, {p}3)
					on conflict (id) do update set touched_at = {p}3;
				"
			);
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let bytes = if self.store.is_none() {
				Some(arg.bytes.clone())
			} else {
				None
			};
			let params = db::params![id, bytes, now];
			connection
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Return all the children as incomplete.
		let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let incomplete = data.children();

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
