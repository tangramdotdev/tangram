use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn delete_tag(&self, tag: &tg::Tag) -> tg::Result<()> {
		todo!()
	}
}

impl Server {
	pub(crate) async fn handle_delete_tag_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		tag: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let tag = tag.parse()?;
		handle.delete_tag(&tag).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
