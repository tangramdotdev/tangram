use crate::{
	self as tg,
	util::http::{Outgoing, ResponseExt as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub path: tg::Path,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::artifact::Id,
}

impl tg::Artifact {
	pub async fn check_in<H>(handle: &H, path: tg::Path) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let arg = Arg { path };
		let output = handle.check_in_artifact(arg).await?;
		let artifact = Self::with_id(output.id);
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
		let body = Outgoing::json(arg);
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
