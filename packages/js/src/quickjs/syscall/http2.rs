use {
	super::Result,
	crate::quickjs::{serde::Serde, types::Uint8Array},
};

pub async fn connect(
	authority: String,
	options: Serde<crate::http2::ConnectOptions>,
) -> Result<usize> {
	let Serde(options) = options;
	Result(crate::http2::connect(authority, options).await)
}

pub async fn session_request(
	session: usize,
	headers: Serde<Vec<(String, String)>>,
	options: Serde<crate::http2::RequestOptions>,
) -> Result<usize> {
	let Serde(headers) = headers;
	let Serde(options) = options;
	Result(crate::http2::session_request(session, headers, options).await)
}

pub async fn stream_write(stream: usize, bytes: Uint8Array) -> Result<()> {
	Result(crate::http2::stream_write(stream, bytes.into()).await)
}

pub async fn stream_end(stream: usize, bytes: Option<Uint8Array>) -> Result<()> {
	Result(crate::http2::stream_end(stream, bytes.map(Into::into)).await)
}

pub async fn stream_read(stream: usize) -> Result<Option<Serde<crate::http2::StreamEvent>>> {
	Result(
		crate::http2::stream_read(stream)
			.await
			.map(|event| event.map(Serde)),
	)
}

pub async fn stream_close(stream: usize) -> Result<()> {
	Result(crate::http2::stream_close(stream).await)
}

pub async fn session_close(session: usize) -> Result<()> {
	Result(crate::http2::session_close(session).await)
}

pub async fn session_destroy(session: usize, error: Option<String>) -> Result<()> {
	Result(crate::http2::session_destroy(session, error).await)
}
