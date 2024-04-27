use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub items: Vec<tg::root::get::Output>,
}

impl tg::Client {
	pub async fn list_roots(&self, arg: tg::root::list::Arg) -> tg::Result<tg::root::list::Output> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the query"))?;
		let uri = format!("/roots?{query}");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = Outgoing::empty();
		let request = request.body(body).unwrap();
		let response = self.send(request).await?;
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(output)
	}
}
