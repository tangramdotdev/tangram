use crate::{self as tg, Client};
use futures::{Stream, TryStreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use tangram_http::{request::builder::Ext as _, response::Ext as _};

impl Client {
	pub async fn read_pipe(
		&self,
		id: &tg::pipe::Id,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>>> {
		let method = http::Method::POST;
		let uri = format!("/pipes/{id}/read");
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
		Ok(stream)
	}
}
