use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn remove_remote(&self, name: &str) -> tg::Result<()> {
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from remotes
				where name = {p}1;
			",
		);
		let params = db::params![&name];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statemtent"))?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_delete_remote_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		name: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		handle.delete_remote(name).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
