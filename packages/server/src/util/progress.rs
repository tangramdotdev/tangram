use futures::Stream;
use tangram_client as tg;
use tangram_http::{outgoing::response::Ext, Outgoing};
// use tangram_http as http;
use futures::StreamExt as _;

pub fn sse<T>(
	stream: impl Stream<Item = tg::Result<tg::Progress<T>>> + Send + 'static,
) -> http::Response<Outgoing>
where
	T: serde::Serialize,
{
	let stream = stream.map(|result| {
		result.and_then(|progress| {
			let sse: tangram_http::sse::Event = progress.try_into()?;
			Ok::<_, tg::Error>(sse)
		})
	});
	let body = Outgoing::sse(stream);
	http::Response::builder().ok().body(body).unwrap()
}
