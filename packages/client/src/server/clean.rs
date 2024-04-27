use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

impl tg::Client {
	pub async fn clean(&self) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/clean";
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
