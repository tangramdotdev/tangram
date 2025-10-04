use {
	crate as tg,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub pattern: tg::tag::Pattern,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub deleted: Vec<Item>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Item {
	pub tag: tg::Tag,
	pub is_leaf: bool,
}

impl tg::Client {
	pub async fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		let method = http::Method::DELETE;
		let uri = "/tags".to_owned();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
