use {
	crate::{Server, handle::ServerOrProxy},
	indoc::formatdoc,
	tangram_client::{self as tg, prelude::*},
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
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
		handle: &ServerOrProxy,
		request: http::Request<Body>,
		name: &str,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.json().await?;
		handle.put_remote(name, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
