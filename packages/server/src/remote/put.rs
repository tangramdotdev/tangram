use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn put_remote_with_context(
		&self,
		context: &Context,
		name: &str,
		arg: tg::remote::put::Arg,
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
				insert into remotes (name, url)
				values ({p}1, {p}2)
				on conflict (name)
				do update set url = {p}2;
			",
		);
		let params = db::params![&name, &arg.url.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to insert the remote"))?;
		self.remotes.remove(name);
		Ok(())
	}

	pub(crate) async fn handle_put_remote_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		name: &str,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.json().await?;
		self.put_remote_with_context(context, name, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
