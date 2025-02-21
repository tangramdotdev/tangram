use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

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
		let params = db::params![&name, &arg.url];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to insert the remote"))?;
		self.remotes.remove(name);
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_put_remote_request<H>(
		handle: &H,
		request: http::Request<Body>,
		name: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		handle.put_remote(name, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
