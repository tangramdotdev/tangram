use {super::State, bytes::Bytes, std::rc::Rc, tangram_client::prelude::*, tangram_v8::Serde};

pub async fn connect(
	state: Rc<State>,
	args: (String, Serde<crate::http2::ConnectOptions>),
) -> tg::Result<usize> {
	let (authority, Serde(options)) = args;
	state.http2.connect(authority, options).await
}

pub async fn session_request(
	state: Rc<State>,
	args: (
		usize,
		Serde<Vec<(String, String)>>,
		Serde<crate::http2::RequestOptions>,
	),
) -> tg::Result<usize> {
	let (session, Serde(headers), Serde(options)) = args;
	state.http2.session_request(session, headers, options).await
}

pub async fn stream_write(state: Rc<State>, args: (usize, Bytes)) -> tg::Result<()> {
	let (stream, bytes) = args;
	state.http2.stream_write(stream, bytes).await
}

pub async fn stream_end(state: Rc<State>, args: (usize, Option<Bytes>)) -> tg::Result<()> {
	let (stream, bytes) = args;
	state.http2.stream_end(stream, bytes).await
}

pub async fn stream_read(
	state: Rc<State>,
	args: (usize,),
) -> tg::Result<Option<Serde<crate::http2::StreamEvent>>> {
	let (stream,) = args;
	state
		.http2
		.stream_read(stream)
		.await
		.map(|event| event.map(Serde))
}

pub async fn stream_close(state: Rc<State>, args: (usize,)) -> tg::Result<()> {
	let (stream,) = args;
	state.http2.stream_close(stream).await
}

pub async fn session_close(state: Rc<State>, args: (usize,)) -> tg::Result<()> {
	let (session,) = args;
	state.http2.session_close(session).await
}

pub async fn session_destroy(state: Rc<State>, args: (usize, Option<String>)) -> tg::Result<()> {
	let (session, error) = args;
	state.http2.session_destroy(session, error).await
}
