use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Build {
	pub async fn pull<H1, H2>(&self, _handle: &H1, _remote: &H2) -> tg::Result<()>
	where
		H1: tg::Handle,
		H2: tg::Handle,
	{
		Err(tg::error!("unimplemented"))
	}
}

impl tg::Client {
	pub async fn pull_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/pull");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		response.success().await?;
		Ok(())
	}
}
