use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};

impl Server {
	pub async fn delete_remote(&self, name: &str) -> tg::Result<()> {
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
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statemtent"))?;
		self.remotes.remove(name);
		Ok(())
	}

	pub(crate) async fn handle_delete_remote_request<H>(
		handle: &H,
		_request: http::Request<Body>,
		name: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		handle.delete_remote(name).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
