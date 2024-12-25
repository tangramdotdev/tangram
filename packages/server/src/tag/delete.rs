use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Incoming, Outgoing, outgoing::response::Ext as _};

impl Server {
	pub async fn delete_tag(&self, tag: &tg::Tag) -> tg::Result<()> {
		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Delete the tag.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from tags
				where tag = {p}1;
			"
		);
		let params = db::params![tag];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_delete_tag_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		tag: &[&str],
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		handle.delete_tag(&tag).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
