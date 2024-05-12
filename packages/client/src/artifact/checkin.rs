use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub path: tg::Path,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub artifact: tg::artifact::Id,
}

impl tg::Artifact {
	pub async fn check_in<H>(handle: &H, path: tg::Path) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let arg = Arg { path };
		let output = handle.check_in_artifact(arg).await?;
		let artifact = Self::with_id(output.artifact);
		Ok(artifact)
	}
}

impl tg::Client {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<tg::artifact::checkin::Output> {
		let method = http::Method::POST;
		let uri = "/artifacts/checkin";
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
		let output = response.json().await?;
		Ok(output)
	}
}
