use {
	crate::tg,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Location>,

	pub token: String,
}

impl<O> tg::Process<O> {
	pub async fn cancel(&self) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.cancel_with_handle(handle).await
	}

	pub async fn cancel_with_handle<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if self.pid.is_some() {
			return self
				.signal_with_handle(handle, tg::process::Signal::SIGTERM)
				.await;
		}
		self.ensure_location_with_handle(handle).await?;
		let id = self.id();
		let location = self
			.locations()
			.and_then(|locations| locations.to_location());
		let token = self
			.token()
			.ok_or_else(|| tg::error!("missing token"))?
			.clone();
		handle.cancel_process(id, Arg { location, token }).await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn try_cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<()>> {
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
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		Ok(Some(()))
	}
}
