use {
	crate::prelude::*,
	futures::{Stream, TryStreamExt as _},
	serde_with::serde_as,
	tangram_http::body::{BodyStream, Ext as _},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::CommaSeparatedString,
};

#[serde_as]
#[derive(Default, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

impl tg::Client {
	pub async fn try_read_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		arg: Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + use<>>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/pipes/{id}/read?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::ACCEPT,
				mime::APPLICATION_OCTET_STREAM.to_string(),
			)
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			if matches!(response.status(), http::StatusCode::NOT_FOUND) {
				return Ok(None);
			}
			let error = response
				.json()
				.await
				.map_err(|source| tg::error!(!source, "failed to parse error"))?;
			return Err(error);
		}
		let body = response
			.into_body()
			.map_err(|source| tg::error!(!source, "failed to read the body"));
		let stream = BodyStream::new(body).and_then(|frame| async {
			match frame.into_data() {
				Ok(bytes) => Ok(tg::pipe::Event::Chunk(bytes)),
				Err(frame) => {
					let trailers = frame.into_trailers().unwrap();
					let event = trailers
						.get("x-tg-event")
						.ok_or_else(|| tg::error!("missing event"))?
						.to_str()
						.map_err(|source| tg::error!(!source, "invalid event"))?;
					match event {
						"end" => Ok(tg::pipe::Event::End),
						"error" => {
							let data = trailers
								.get("x-tg-data")
								.ok_or_else(|| tg::error!("missing data"))?
								.to_str()
								.map_err(|source| tg::error!(!source, "invalid data"))?;
							let error = serde_json::from_str(data).map_err(|source| {
								tg::error!(!source, "failed to deserialize the header value")
							})?;
							Err(error)
						},
						_ => Err(tg::error!("invalid event")),
					}
				},
			}
		});
		Ok(Some(stream))
	}
}
