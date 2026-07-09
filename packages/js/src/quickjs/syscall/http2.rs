use {
	super::Result,
	crate::quickjs::{
		StateHandle,
		serde::Serde,
		types::{OptionNull, Uint8Array},
	},
	rquickjs as qjs,
};

pub async fn connect(
	ctx: qjs::Ctx<'_>,
	authority: String,
	options: Serde<crate::http2::ConnectOptions>,
) -> Result<usize> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(options) = options;
	Result(state.http2.connect(authority, options).await)
}

pub async fn session_request(
	ctx: qjs::Ctx<'_>,
	session: usize,
	headers: Serde<Vec<(String, String)>>,
	options: Serde<crate::http2::RequestOptions>,
) -> Result<usize> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(headers) = headers;
	let Serde(options) = options;
	Result(state.http2.session_request(session, headers, options).await)
}

pub async fn stream_write(ctx: qjs::Ctx<'_>, stream: usize, bytes: Uint8Array) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.http2.stream_write(stream, bytes.into()).await)
}

pub async fn stream_end(ctx: qjs::Ctx<'_>, stream: usize, bytes: Option<Uint8Array>) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.http2.stream_end(stream, bytes.map(Into::into)).await)
}

pub async fn stream_read(
	ctx: qjs::Ctx<'_>,
	stream: usize,
) -> Result<OptionNull<Serde<crate::http2::StreamEvent>>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(
		state
			.http2
			.stream_read(stream)
			.await
			.map(|event| OptionNull(event.map(Serde))),
	)
}

pub async fn stream_close(ctx: qjs::Ctx<'_>, stream: usize) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.http2.stream_close(stream).await)
}

pub async fn session_close(ctx: qjs::Ctx<'_>, session: usize) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.http2.session_close(session).await)
}

pub async fn session_destroy(
	ctx: qjs::Ctx<'_>,
	session: usize,
	error: Option<String>,
) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.http2.session_destroy(session, error).await)
}
