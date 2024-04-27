use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub query: String,
}

pub type Output = Vec<String>;

impl tg::Client {
	pub async fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> tg::Result<tg::package::list::Output> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the query"))?;
		let uri = format!("/packages?{query}");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		let response = response.success().await?;
		let output = response.json().await?;
		Ok(output)
	}
}
