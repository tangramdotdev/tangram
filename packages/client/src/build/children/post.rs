use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub child: tg::build::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl tg::Build {
	pub async fn add_child<H>(
		&self,
		handle: &H,
		arg: tg::build::children::post::Arg,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		handle.add_build_child(id, arg).await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn add_build_child(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::post::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/children");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
