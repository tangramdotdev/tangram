use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn delete_remote_with_context(
		&self,
		context: &Context,
		name: &str,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

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
		&self,
		_request: http::Request<Body>,
		context: &Context,
		name: &str,
	) -> tg::Result<http::Response<Body>> {
		self.delete_remote_with_context(context, name).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
