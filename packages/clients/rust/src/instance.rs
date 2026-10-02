use {
	crate::prelude::*,
	futures::{Stream, StreamExt as _, stream::BoxStream},
	std::sync::OnceLock,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

mod checkpoint;
mod either;
mod ext;
mod grant;
mod group;
mod module;
mod object;
mod organization;
mod process;
mod remote;
mod runner;
mod sandbox;
mod tag;
mod user;
mod watch;

pub use self::{
	checkpoint::Checkpoint, ext::Ext, grant::Grant, group::Group, module::Module, object::Object,
	organization::Organization, process::Process, remote::Remote, runner::Runner, sandbox::Sandbox,
	tag::Tag, user::User, watch::Watch,
};

pub mod dynamic;
pub mod erased;

pub static INSTANCE: OnceLock<tg::Client> = OnceLock::new();

pub fn init() -> tg::Result<&'static tg::Client> {
	let client = tg::Client::with_env(tg::Arg::default())?;
	init_with(client)
}

pub fn init_with(client: tg::Client) -> tg::Result<&'static tg::Client> {
	if let Some(instance) = INSTANCE.get() {
		return Ok(instance);
	}
	match INSTANCE.set(client) {
		Ok(()) | Err(_) => Ok(INSTANCE.get().unwrap()),
	}
}

#[must_use]
pub fn try_instance() -> Option<&'static tg::Client> {
	INSTANCE.get()
}

pub(crate) fn instance() -> tg::Result<&'static tg::Client> {
	try_instance().ok_or_else(|| tg::error!("tangram is not initialized; call tg::init() first"))
}

/// The API of a Tangram instance, implemented directly by servers and through transport by clients.
///
/// Allows shared code to run against clients and servers, bypassing transport when using a server directly.
pub trait Instance:
	Checkpoint
	+ Grant
	+ Group
	+ Module
	+ Object
	+ Organization
	+ Process
	+ Remote
	+ Runner
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

	/// Collect all pages, using the limit as the page size and the cursor as the starting point.
	fn children_all(
		&self,
		mut arg: tg::children::Arg,
	) -> impl Future<Output = tg::Result<tg::children::Output>> + Send {
		async move {
			let mut output = self.children(arg.clone()).await?;
			while let Some(cursor) = output.cursor.take() {
				arg.cursor = Some(cursor);
				let page = self.children(arg.clone()).await?;
				output.data.extend(page.data);
				output.cursor = page.cursor;
			}
			Ok(output)
		}
	}

	fn children(
		&self,
		arg: tg::children::Arg,
	) -> impl Future<Output = tg::Result<tg::children::Output>> + Send;

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

	/// Collect all pages, using the limit as the page size and the cursor as the starting point.
	fn list_all(
		&self,
		mut arg: tg::list::Arg,
	) -> impl Future<Output = tg::Result<tg::list::Output>> + Send {
		async move {
			let mut output = self.list(arg.clone()).await?;
			while let Some(cursor) = output.cursor.take() {
				arg.cursor = Some(cursor);
				let page = self.list(arg.clone()).await?;
				output.data.extend(page.data);
				output.cursor = page.cursor;
			}
			Ok(output)
		}
	}

	fn list(&self, arg: tg::list::Arg)
	-> impl Future<Output = tg::Result<tg::list::Output>> + Send;

	/// Collect all pages, using the limit as the page size and the cursor as the starting point.
	fn match_all(
		&self,
		mut arg: tg::match_::Arg,
	) -> impl Future<Output = tg::Result<tg::match_::Output>> + Send {
		async move {
			let mut output = self.match_(arg.clone()).await?;
			while let Some(cursor) = output.cursor.take() {
				arg.cursor = Some(cursor);
				let page = self.match_(arg.clone()).await?;
				output.data.extend(page.data);
				output.cursor = page.cursor;
			}
			Ok(output)
		}
	}

	fn match_(
		&self,
		arg: tg::match_::Arg,
	) -> impl Future<Output = tg::Result<tg::match_::Output>> + Send;

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> impl Future<
		Output = tg::Result<(
			tg::pull::Header,
			impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
		)>,
	> + Send;

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<(
			tg::push::Header,
			impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
		)>,
	> + Send;

	fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> impl Future<
		Output = tg::Result<(
			tg::sync::Header,
			impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static,
		)>,
	> + Send;

	fn get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::Referent<tg::get::Node>>>>
			+ Send
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
						.map(|output| tg::progress::Event::Output(output.referent))
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

impl tg::Instance for tg::Client {
	fn arg(&self) -> tg::Arg {
		self.session(&self.context).arg()
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

	async fn children(&self, arg: tg::children::Arg) -> tg::Result<tg::children::Output> {
		self.session(&self.context).children(arg).await
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

	async fn list(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		self.session(&self.context).list(arg).await
	}

	async fn match_(&self, arg: tg::match_::Arg) -> tg::Result<tg::match_::Output> {
		self.session(&self.context).match_(arg).await
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
	) -> tg::Result<(
		tg::pull::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	)> {
		self.session(&self.context).pull(arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<(
		tg::push::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	)> {
		self.session(&self.context).push(arg).await
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<(
		tg::sync::Header,
		impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static,
	)> {
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
