use {
	crate::tg,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	pub lease: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub released: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub lease: Option<String>,
	pub location: Option<tg::location::Arg>,
}

impl<O> tg::Process<O> {
	pub async fn cancel(&self, options: tg::process::cancel::Options) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.cancel_with_handle(handle, options).await
	}

	pub async fn cancel_with_handle<H>(
		&self,
		handle: &H,
		options: tg::process::cancel::Options,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if self.id().is_left() {
			let options = tg::process::signal::Options::default();
			self.signal_with_handle(handle, tg::process::Signal::SIGTERM, options)
				.await?;
			self.detach();
			return Ok(());
		}
		let tg::process::cancel::Options { lease, location } = options;
		if location.is_none() && self.location().is_none() {
			self.ensure_location_with_handle(handle).await?;
		}
		let id = self.id().unwrap_right();
		let location = location.or_else(|| self.location());
		let lease = lease
			.or_else(|| self.lease().cloned())
			.ok_or_else(|| tg::error!("missing lease"))?;
		handle.cancel_process(id, Arg { location, lease }).await?;
		self.detach();

		Ok(())
	}
}

impl tg::Session {
	pub async fn try_cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		let method = http::Method::POST;
		let path = format!("/processes/{id}/cancel");
		let uri = Uri::builder()
			.path(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(Some(output))
	}
}
