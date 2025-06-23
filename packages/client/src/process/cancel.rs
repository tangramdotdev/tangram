use crate::tg;
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	pub remote: Option<String>,
	pub token: String,
}

impl tg::Process {
	pub async fn cancel<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let remote = self.remote.clone();
		let token = self
			.token()
			.ok_or_else(|| tg::error!("missing token"))?
			.clone();
		handle.cancel_process(id, Arg { remote, token }).await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/processes/{id}/cancel?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
