use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	pub outcome: tg::build::outcome::Data,
	pub remote: Option<String>,
}

impl tg::Build {
	pub async fn finish<H>(&self, handle: &H, arg: tg::build::finish::Arg) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		handle.finish_build(id, arg).await?;
		Ok(())
	}

	pub async fn cancel<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let arg = tg::build::finish::Arg {
			outcome: tg::build::outcome::Data::Canceled,
			remote: None,
		};
		handle.finish_build(id, arg).await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/finish");
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
