use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn put_root(&self, name: &str, arg: tg::root::put::Arg) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Insert the root.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into roots (name, item)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![name, arg.item];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_put_root_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		name: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		handle.put_root(name, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
