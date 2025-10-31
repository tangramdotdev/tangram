use {
	crate as tg,
	std::path::PathBuf,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub data: Vec<Item>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Item {
	pub path: PathBuf,
}

impl tg::Client {
	pub async fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> tg::Result<tg::watch::list::Output> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/watches?{query}");
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
