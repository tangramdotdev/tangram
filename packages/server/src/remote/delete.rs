use {
	crate::{Server, handle::ServerOrProxy},
	indoc::formatdoc,
	tangram_client::{self as tg, prelude::*},
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, response::builder::Ext as _},
};

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

	pub(crate) async fn handle_delete_remote_request(
		handle: &ServerOrProxy,
		_request: http::Request<Body>,
		name: &str,
	) -> tg::Result<http::Response<Body>> {
		handle.delete_remote(name).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
