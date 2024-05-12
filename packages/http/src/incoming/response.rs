use crate::{sse, Error, Incoming};
use bytes::Bytes;
use futures::{future, Future, Stream, TryStreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;

pub trait Ext: Sized {
	fn bytes(self) -> impl Future<Output = Result<Bytes, Error>> + Send;

	fn json<T>(self) -> impl Future<Output = Result<T, Error>> + Send
	where
		T: serde::de::DeserializeOwned;

	fn reader(self) -> impl AsyncBufRead + Send + 'static;

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static;
}

impl Ext for http::Response<Incoming> {
	async fn bytes(self) -> Result<Bytes, Error> {
		let collected = self.collect().await?;
		Ok(collected.to_bytes())
	}

	async fn json<T>(self) -> Result<T, Error>
	where
		T: serde::de::DeserializeOwned,
	{
		let bytes = self.bytes().await?;
		let json = serde_json::from_slice(&bytes)?;
		Ok(json)
	}

	fn reader(self) -> impl AsyncBufRead + Send + 'static {
		StreamReader::new(
			BodyStream::new(self.into_body())
				.try_filter_map(|frame| future::ok(frame.into_data().ok()))
				.map_err(std::io::Error::other),
		)
	}

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static {
		sse::decode(self.reader()).err_into()
	}
}
