use {
	futures::{
		StreamExt as _,
		stream::{self, BoxStream},
	},
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_futures::stream::Ext as _,
};

/// End a stream with an end event when its timeout expires, which tells the client that the stream is complete, because a stream that ends without one means that the connection was lost, and the client reconnects.
pub fn end_after_timeout<T>(
	stream: BoxStream<'static, tg::Result<T>>,
	timeout: Option<Duration>,
	end: T,
	is_end: fn(&T) -> bool,
) -> BoxStream<'static, tg::Result<T>>
where
	T: Send + 'static,
{
	let Some(timeout) = timeout.filter(|timeout| !timeout.is_zero()) else {
		return stream;
	};
	let timer = stream::once(async move {
		tokio::time::sleep(timeout).await;
		Ok(end)
	});
	stream::select(stream, timer)
		.take_while_inclusive(move |result| {
			let done = matches!(result, Ok(event) if is_end(event));
			std::future::ready(!done)
		})
		.boxed()
}
