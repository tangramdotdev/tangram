use {
	crate as tg,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Size {
	pub rows: u16,
	pub cols: u16,
}

impl tg::Client {
	pub async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		let method = http::Method::GET;
		let uri = format!("/ptys/{id}/size");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		self.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the response"))?
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))
	}
}
