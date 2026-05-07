use {
	crate::prelude::*,
	futures::{Stream, StreamExt as _, stream::BoxStream},
	std::sync::OnceLock,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

mod either;
mod ext;
mod module;
mod object;
mod process;
mod remote;
mod sandbox;
mod tag;
mod user;
mod watch;

pub use self::{
	ext::Ext, module::Module, object::Object, process::Process, remote::Remote, sandbox::Sandbox,
	tag::Tag, user::User, watch::Watch,
};

pub mod dynamic;
pub mod erased;

pub static HANDLE: OnceLock<tg::Client> = OnceLock::new();

pub fn init() -> tg::Result<&'static tg::Client> {
	let client = tg::Client::with_env(tg::Arg::default())?;
	init_with(client)
}

pub fn init_with(client: tg::Client) -> tg::Result<&'static tg::Client> {
	if let Some(handle) = HANDLE.get() {
		return Ok(handle);
	}
	match HANDLE.set(client) {
		Ok(()) | Err(_) => Ok(HANDLE.get().unwrap()),
	}
}

#[must_use]
pub fn try_handle() -> Option<&'static tg::Client> {
	HANDLE.get()
}

pub(crate) fn handle() -> tg::Result<&'static tg::Client> {
	try_handle().ok_or_else(|| tg::error!("tangram is not initialized; call tg::init() first"))
}

pub trait Handle:
	Module
	+ Object
	+ Process
	+ Remote
	+ Sandbox
	+ Tag
	+ User
	+ Watch
	+ Clone
	+ Unpin
	+ Send
	+ Sync
	+ 'static
{
	fn arg(&self) -> tg::Arg;

	fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send;

	fn check(
		&self,
		arg: tg::check::Arg,
	) -> impl Future<Output = tg::Result<tg::check::Output>> + Send;

	fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
		>,
	> + Send;

	fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
		>,
	> + Send;

	fn clean(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
		>,
	> + Send;

	fn document(
		&self,
		arg: tg::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> + Send;

	fn format(&self, arg: tg::format::Arg) -> impl Future<Output = tg::Result<()>> + Send;

	fn health(&self, arg: tg::health::Arg) -> impl Future<Output = tg::Result<tg::Health>> + Send;

	fn index(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send;

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
		>,
	> + Send;

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
		>,
	> + Send;

	fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static>,
	> + Send;

	fn get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<
				Item = tg::Result<
					tg::progress::Event<
						tg::Referent<tg::Either<tg::graph::Edge<tg::Object>, tg::Process>>,
					>,
				>,
			> + Send
			+ 'static,
		>,
	> + Send {
		async move {
			let stream = self.try_get(reference, arg).await?;
			let reference = reference.clone();
			let stream = stream.map(move |event_result| {
				event_result.and_then(|event| match event {
					tg::progress::Event::Log(log) => Ok(tg::progress::Event::Log(log)),
					tg::progress::Event::Diagnostic(diagnostic) => {
						Ok(tg::progress::Event::Diagnostic(diagnostic))
					},
					tg::progress::Event::Indicators(indicators) => {
						Ok(tg::progress::Event::Indicators(indicators))
					},
					tg::progress::Event::Output(output) => output
						.map(|output| {
							let referent = output.referent.map(|item| {
								item.map_left(|edge| match edge {
									tg::graph::data::Edge::Object(id) => {
										tg::graph::Edge::Object(tg::Object::with_id(id))
									},
									tg::graph::data::Edge::Pointer(pointer) => {
										tg::graph::Edge::Pointer(tg::graph::Pointer {
											graph: pointer.graph.map(tg::Graph::with_id),
											index: pointer.index,
											kind: pointer.kind,
										})
									},
								})
								.map_right(|id| tg::Process::new(id, None, None, None, None, None))
							});
							tg::progress::Event::Output(referent)
						})
						.ok_or_else(|| tg::error!(%reference, "failed to get the reference")),
				})
			});
			Ok(stream)
		}
	}

	fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>>
			+ Send
			+ 'static,
		>,
	> + Send;

	fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>,
		>,
	> + Send;

	fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::write::Output>> + Send;
}

impl tg::Handle for tg::Client {
	fn arg(&self) -> tg::Arg {
		self.session(&self.context).arg()
	}

	async fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.session(&self.context).cache(arg).await
	}

	async fn check(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		self.session(&self.context).check(arg).await
	}

	async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		self.session(&self.context).checkin(arg).await
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		self.session(&self.context).checkout(arg).await
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		self.session(&self.context).clean().await
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		self.session(&self.context).document(arg).await
	}

	async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		self.session(&self.context).format(arg).await
	}

	async fn health(&self, arg: tg::health::Arg) -> tg::Result<tg::Health> {
		self.session(&self.context).health(arg).await
	}

	async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.session(&self.context).index().await
	}

	async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		self.session(&self.context).lsp(input, output).await
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	> {
		self.session(&self.context).pull(arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	> {
		self.session(&self.context).push(arg).await
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static> {
		self.session(&self.context).sync(arg, stream).await
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		self.session(&self.context).try_get(reference, arg).await
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		self.session(&self.context).try_read_blob_stream(arg).await
	}

	async fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		self.session(&self.context).write(arg, reader).await
	}
}
