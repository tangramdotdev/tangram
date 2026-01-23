use {
	crate::tg,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::CommaSeparatedString,
};

#[serde_as]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,

	pub token: String,
}

impl tg::Process {
	pub async fn cancel<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		let (local, remotes) = match self.remote.clone() {
			Some(remote) => (None, Some(vec![remote])),
			None => (Some(true), None),
		};
		let token = self
			.token()
			.ok_or_else(|| tg::error!("missing token"))?
			.clone();
		handle
			.cancel_process(
				id,
				Arg {
					local,
					remotes,
					token,
				},
			)
			.await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/processes/{id}/cancel?{query}");
		let response = self
			.send(|| {
				http::request::Builder::default()
					.method(http::Method::POST)
					.uri(uri.clone())
					.empty()
					.unwrap()
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		Ok(())
	}
}
