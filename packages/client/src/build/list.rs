use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub limit: Option<u64>,
	pub order: Option<Order>,
	pub status: Option<tg::build::Status>,
	pub target: Option<tg::target::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Order {
	#[serde(rename = "created_at")]
	CreatedAt,
	#[serde(rename = "created_at.desc")]
	CreatedAtDesc,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub items: Vec<tg::build::get::Output>,
}

impl tg::Client {
	pub async fn list_builds(
		&self,
		arg: tg::build::list::Arg,
	) -> tg::Result<tg::build::list::Output> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds?{query}");
		let request = http::request::Builder::default().method(method).uri(uri);
		let request = request.empty().unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
