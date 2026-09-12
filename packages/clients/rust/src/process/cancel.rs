use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub lease: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Output {
	#[tangram_serialize(id = 0)]
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
		let handle = self.handle_with_handle(handle);
		let handle = &handle;
		if self.id().is_left() {
			let options = tg::process::signal::Options::default();
			self.signal_with_handle(handle, tg::process::Signal::SIGTERM, options)
				.await?;
			self.disarm();
			return Ok(());
		}
		let tg::process::cancel::Options { lease, location } = options;
		if self
			.0
			.connection
			.as_ref()
			.is_none_or(tg::process::connect::Connection::detached)
			&& location.is_none()
			&& self.location().is_none()
		{
			self.ensure_location_with_handle(handle).await?;
		}
		let id = self.id().unwrap_right();
		let location = location.or_else(|| self.location());
		let lease = lease
			.or_else(|| self.lease().cloned())
			.ok_or_else(|| tg::error!("missing lease"))?;
		let arg = Arg { lease, location };
		handle.cancel_process(id, arg).await?;
		self.disarm();

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
		let uri = Uri::builder().path(&path).build().unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let request = tangram_http::request::with_query_params(request, &arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?;
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
