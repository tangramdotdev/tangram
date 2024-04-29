use crate as tg;
use tangram_http::Outgoing;

impl tg::Client {
	pub async fn stop(&self) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/stop";
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		self.send(request).await.ok();
		Ok(())
	}
}
