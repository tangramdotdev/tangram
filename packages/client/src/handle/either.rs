use crate as tg;
use futures::{Future, FutureExt as _, Stream};
use tangram_either::Either;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

impl<L, R> tg::Handle for Either<L, R>
where
	L: tg::Handle,
	R: tg::Handle,
{
	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkin::Output>>>
				+ Send
				+ 'static,
		>,
	> {
		match self {
			Either::Left(s) => s
				.check_in_artifact(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.check_in_artifact(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

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
	> {
		match self {
			Either::Left(s) => s
				.check_out_artifact(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.check_out_artifact(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::create::Output>> {
		match self {
			Either::Left(s) => s.create_blob(reader).left_future(),
			Either::Right(s) => s.create_blob(reader).right_future(),
		}
	}

	fn try_read_blob_stream(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::blob::read::Event>> + Send + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_read_blob_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_read_blob_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_spawn_command(
		&self,
		id: &tg::command::Id,
		arg: tg::command::spawn::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::command::spawn::Output>>> {
		match self {
			Either::Left(s) => s.try_spawn_command(id, arg).left_future(),
			Either::Right(s) => s.try_spawn_command(id, arg).right_future(),
		}
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.lsp(input, output).left_future(),
			Either::Right(s) => s.lsp(input, output).right_future(),
		}
	}

	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		match self {
			Either::Left(s) => s.try_get_object_metadata(id).left_future(),
			Either::Right(s) => s.try_get_object_metadata(id).right_future(),
		}
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_object(id).left_future(),
			Either::Right(s) => s.try_get_object(id).right_future(),
		}
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		match self {
			Either::Left(s) => s.put_object(id, arg).left_future(),
			Either::Right(s) => s.put_object(id, arg).right_future(),
		}
	}

	fn push_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send {
		match self {
			Either::Left(s) => s
				.push_object(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.push_object(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn pull_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send {
		match self {
			Either::Left(s) => s
				.pull_object(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.pull_object(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn check_package(
		&self,
		arg: tg::package::check::Arg,
	) -> impl Future<Output = tg::Result<tg::package::check::Output>> + Send {
		match self {
			Either::Left(s) => s.check_package(arg).left_future(),
			Either::Right(s) => s.check_package(arg).right_future(),
		}
	}

	fn document_package(
		&self,
		arg: tg::package::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> + Send {
		match self {
			Either::Left(s) => s.document_package(arg).left_future(),
			Either::Right(s) => s.document_package(arg).right_future(),
		}
	}

	fn format_package(
		&self,
		arg: tg::package::format::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Either::Left(s) => s.format_package(arg).left_future(),
			Either::Right(s) => s.format_package(arg).right_future(),
		}
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_process(id).left_future(),
			Either::Right(s) => s.try_get_process(id).right_future(),
		}
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<tg::process::put::Output>> {
		match self {
			Either::Left(s) => s.put_process(id, arg).left_future(),
			Either::Right(s) => s.put_process(id, arg).right_future(),
		}
	}

	fn push_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		match self {
			Either::Left(s) => s
				.push_process(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.push_process(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn pull_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		match self {
			Either::Left(s) => s
				.pull_process(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.pull_process(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::dequeue::Output>>> {
		match self {
			Either::Left(s) => s.try_dequeue_process(arg).left_future(),
			Either::Right(s) => s.try_dequeue_process(arg).right_future(),
		}
	}

	fn try_start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> impl Future<Output = tg::Result<tg::process::start::Output>> + Send {
		match self {
			Either::Left(s) => s.try_start_process(id, arg).left_future(),
			Either::Right(s) => s.try_start_process(id, arg).right_future(),
		}
	}

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_process_status_stream(id)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_process_status_stream(id)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

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
	> {
		match self {
			Either::Left(s) => s
				.try_get_process_children_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_process_children_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	async fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_process_log_stream(id, arg)
				.await
				.map(|option| option.map(futures::StreamExt::left_stream)),
			Either::Right(s) => s
				.try_get_process_log_stream(id, arg)
				.await
				.map(|option| option.map(futures::StreamExt::right_stream)),
		}
	}

	fn try_post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<tg::process::log::post::Output>> {
		match self {
			Either::Left(s) => s.try_post_process_log(id, arg).left_future(),
			Either::Right(s) => s.try_post_process_log(id, arg).right_future(),
		}
	}

	fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<tg::process::finish::Output>> {
		match self {
			Either::Left(s) => s.try_finish_process(id, arg).left_future(),
			Either::Right(s) => s.try_finish_process(id, arg).right_future(),
		}
	}

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Either::Left(s) => s.touch_process(id, arg).left_future(),
			Either::Right(s) => s.touch_process(id, arg).right_future(),
		}
	}

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> + Send {
		match self {
			Either::Left(s) => s.heartbeat_process(id, arg).left_future(),
			Either::Right(s) => s.heartbeat_process(id, arg).right_future(),
		}
	}

	fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> impl Future<
		Output = tg::Result<Option<tg::Referent<Either<tg::process::Id, tg::object::Id>>>>,
	> + Send {
		match self {
			Either::Left(s) => s.try_get_reference(reference).left_future(),
			Either::Right(s) => s.try_get_reference(reference).right_future(),
		}
	}

	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		match self {
			Either::Left(s) => s.list_remotes(arg).left_future(),
			Either::Right(s) => s.list_remotes(arg).right_future(),
		}
	}

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_remote(name).left_future(),
			Either::Right(s) => s.try_get_remote(name).right_future(),
		}
	}

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.put_remote(name, arg).left_future(),
			Either::Right(s) => s.put_remote(name, arg).right_future(),
		}
	}

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.delete_remote(name).left_future(),
			Either::Right(s) => s.delete_remote(name).right_future(),
		}
	}

	fn get_js_runtime_doc(&self) -> impl Future<Output = tg::Result<serde_json::Value>> {
		match self {
			Either::Left(s) => s.get_js_runtime_doc().left_future(),
			Either::Right(s) => s.get_js_runtime_doc().right_future(),
		}
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> {
		match self {
			Either::Left(s) => s.health().left_future(),
			Either::Right(s) => s.health().right_future(),
		}
	}

	fn clean(&self) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.clean().left_future(),
			Either::Right(s) => s.clean().right_future(),
		}
	}

	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::list::Output>> {
		match self {
			Either::Left(s) => s.list_tags(arg).left_future(),
			Either::Right(s) => s.list_tags(arg).right_future(),
		}
	}

	fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_tag(pattern).left_future(),
			Either::Right(s) => s.try_get_tag(pattern).right_future(),
		}
	}

	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.put_tag(tag, arg).left_future(),
			Either::Right(s) => s.put_tag(tag, arg).right_future(),
		}
	}

	fn delete_tag(&self, tag: &tg::Tag) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.delete_tag(tag).left_future(),
			Either::Right(s) => s.delete_tag(tag).right_future(),
		}
	}

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			Either::Left(s) => s.get_user(token).left_future(),
			Either::Right(s) => s.get_user(token).right_future(),
		}
	}
}
