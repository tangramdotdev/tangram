use {
	crate as tg,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(
	Clone,
	Debug,
	Default,
	PartialEq,
	PartialOrd,
	Eq,
	Hash,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Metadata {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub depth: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub weight: Option<u64>,
}

impl tg::Client {
	pub async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/objects/{id}/metadata?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let metadata = response.json().await?;
		Ok(Some(metadata))
	}
}
