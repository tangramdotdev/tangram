use crate::{self as tg, util::http::empty};

impl tg::Client {
	pub async fn stop(&self) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/stop";
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		self.send(request).await.ok();
		Ok(())
	}
}
