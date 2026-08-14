use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
}

impl tg::Object {
	pub async fn touch(&self, options: tg::object::touch::Options) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.touch_with_handle(handle, options).await
	}

	pub async fn touch_with_handle<H>(
		&self,
		handle: &H,
		options: tg::object::touch::Options,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		self.try_touch_with_handle(handle, options)
			.await?
			.ok_or_else(|| tg::error!("failed to touch the object"))
	}

	pub async fn try_touch(&self, options: tg::object::touch::Options) -> tg::Result<Option<()>> {
		let handle = tg::handle()?;
		self.try_touch_with_handle(handle, options).await
	}

	pub async fn try_touch_with_handle<H>(
		&self,
		handle: &H,
		options: tg::object::touch::Options,
	) -> tg::Result<Option<()>>
	where
		H: tg::Handle,
	{
		let state = self.state();
		let arg = tg::object::touch::Arg {
			location: options
				.location
				.or_else(|| state.location().map(Into::into)),
			tokens: state.tokens(),
		};
		handle.try_touch_object(&self.id(), arg).await
	}
}

impl tg::Session {
	pub async fn try_touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<Option<()>> {
		let method = http::Method::POST;
		let uri = format!("/objects/{id}/touch");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
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
		Ok(Some(()))
	}
}
