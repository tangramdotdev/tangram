use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::tag::put::Arg {
				remote: None,
				..arg.clone()
			};
			remote.put_tag(tag, arg).await?;
		}

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Insert the tag.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into tags (tag, item)
				values ({p}1, {p}2)
				on conflict (tag) do update set item = {p}2;
			"
		);
		let params = db::params![tag, arg.item];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_put_tag_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		tag: &[&str],
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let arg = request.json().await?;
		handle.put_tag(&tag, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
