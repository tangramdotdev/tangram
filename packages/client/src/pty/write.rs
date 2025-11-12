use {
	crate::prelude::*,
	futures::{StreamExt as _, stream::BoxStream},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Default, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	pub master: bool,
}

impl tg::Client {
	pub async fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: Arg,
		stream: BoxStream<'static, tg::Result<tg::pty::Event>>,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/ptys/{id}/write?{query}");

		// Create the body.
		let stream = stream.map(|e| match e {
			Ok(e) => e.try_into(),
			Err(e) => e.try_into(),
		});

		// Create the request.
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.sse(stream)
			.unwrap();

		// Send the request.
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response
				.json()
				.await
				.map_err(|source| tg::error!(!source, "failed to deserialize the error response"))?;
			return Err(error);
		}

		Ok(())
	}
}
