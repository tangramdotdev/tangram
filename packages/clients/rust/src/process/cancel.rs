use {
	crate::tg,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
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

impl<O> tg::Process<O> {
	pub async fn cancel<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if self.pid.is_some() {
			return self.signal(handle, tg::process::Signal::SIGTERM).await;
		}
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
		let method = http::Method::POST;
		let path = format!("/processes/{id}/cancel");
		let uri = Uri::builder()
			.path(&path)
			.query_params(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
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
