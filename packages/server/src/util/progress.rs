use futures::Stream;
use futures::StreamExt as _;
use tangram_client as tg;
use tangram_http::{outgoing::response::Ext, Outgoing};

pub fn sse<T>(
	stream: impl Stream<Item = tg::Result<tg::Progress<T>>> + Send + 'static,
) -> http::Response<Outgoing>
where
	T: serde::Serialize,
{
	let stream = stream.map(|result| match result {
		Ok(progress) => progress.try_into(),
		Err(error) => {
			let data = serde_json::to_string(&error)
				.map_err(|source| tg::error!(!source, "failed to serialize error"))?;
			let event = tangram_http::sse::Event {
				event: Some("error".to_owned()),
				data,
				..Default::default()
			};
			Ok(event)
		},
	});
	let body = Outgoing::sse(stream);
	http::Response::builder().ok().body(body).unwrap()
}
