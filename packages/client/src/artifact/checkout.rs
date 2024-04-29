use crate as tg;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub bundle: bool,
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub force: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub path: tg::Path,
}

impl tg::Artifact {
	pub async fn check_out<H>(&self, handle: &H, arg: Arg) -> tg::Result<Output>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let output = handle.check_out_artifact(&id, arg).await?;
		Ok(output)
	}
}

impl tg::Client {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<tg::artifact::checkout::Output> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/checkout");
		let body = Outgoing::json(arg);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
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
