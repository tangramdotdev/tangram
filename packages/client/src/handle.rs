use crate as tg;
use futures::Stream;
use std::pin::Pin;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

mod either;
mod ext;

pub use self::ext::Ext;

pub trait Handle: Clone + Unpin + Send + Sync + 'static {
	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkin::Output>>>
			+ Send
			+ 'static,
		>,
	> + Send;

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>
			+ Send
			+ 'static,
		>,
	> + Send;

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::create::Output>> + Send;

	fn try_read_blob_stream(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::blob::read::Event>> + Send + 'static>,
		>,
	> + Send;

	fn import(
		&self,
		arg: tg::import::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Item>> + Send + 'static>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>,
	> + Send;

	fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>,
	> + Send;

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send;

	fn pull(
		&self,
		arg: tg::pull::Arg,
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

	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> + Send;

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> + Send;

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn check_package(
		&self,
		arg: tg::package::check::Arg,
	) -> impl Future<Output = tg::Result<tg::package::check::Output>> + Send;

	fn document_package(
		&self,
		arg: tg::package::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> + Send;

	fn format_package(
		&self,
		arg: tg::package::format::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::spawn::Output>>> + Send;

	fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
			>,
		>,
	> + Send;

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> + Send;

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> + Send;

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::dequeue::Output>>> + Send;

	fn try_start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> impl Future<Output = tg::Result<tg::process::start::Output>> + Send;

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static,
			>,
		>,
	> + Send;

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> + Send;

	fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static,
			>,
		>,
	> + Send;

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> + Send;

	fn try_post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<tg::process::log::post::Output>> + Send;

	fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<tg::process::finish::Output>> + Send;

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> + Send;

	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> + Send;

	fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>,
	> + Send;

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> + Send;

	fn delete_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> + Send;

	fn read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>,
	> + Send;

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::reference::get::Output>>>>
			+ Send
			+ 'static,
		>,
	> + Send;

	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> + Send;

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> + Send;

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> + Send;

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> + Send;

	fn index(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send;

	fn clean(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send;

	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::list::Output>> + Send;

	fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> + Send;

	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_tag(&self, tag: &tg::Tag) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> + Send;
}
