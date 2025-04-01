use crate::{self as tg, Client};
use futures::{Stream, TryStreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Default, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub master: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl Client {
	pub async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		let method = http::Method::GET;
		let uri = format!("/ptys/{id}/window");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		self.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the response"))?
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))
	}

	pub async fn read_pty(
		&self,
		id: &tg::pty::Id,
		arg: Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/ptys/{id}?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			if matches!(response.status(), http::StatusCode::NOT_FOUND) {
				return Err(tg::error!(%id, "not found"));
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
				Ok(bytes) => Ok(tg::pty::Event::Chunk(bytes)),
				Err(frame) => {
					let trailers = frame.into_trailers().unwrap();
					let event = trailers
						.get("x-tg-event")
						.ok_or_else(|| tg::error!("missing event"))?
						.to_str()
						.map_err(|source| tg::error!(!source, "invalid event"))?;
					match event {
						"size" => {
							let data = trailers
								.get("x-tg-data")
								.ok_or_else(|| tg::error!("missing data"))?
								.to_str()
								.map_err(|source| tg::error!(!source, "invalid data"))?;
							let size = serde_json::from_str(data).map_err(|source| {
								tg::error!(!source, "failed to deserialize the header value")
							})?;
							Ok(tg::pty::Event::Size(size))
						},
						"end" => Ok(tg::pty::Event::End),
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
		Ok(stream)
	}
}
