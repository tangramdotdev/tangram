use crate as tg;
use tangram_http::outgoing::request::Ext as _;

impl tg::Client {
	pub async fn stop(&self) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/stop";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		self.send(request).await.ok();
		Ok(())
	}
}
