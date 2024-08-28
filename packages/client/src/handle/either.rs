use crate::{self as tg, handle::Ext as _};
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
			impl Stream<Item = tg::Result<tg::Progress<tg::artifact::Id>>> + Send + 'static,
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
			impl Stream<Item = tg::Result<tg::Progress<tg::Path>>> + Send + 'static,
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

	fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<tg::build::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_build(id).left_future(),
			Either::Right(s) => s.try_get_build(id).right_future(),
		}
	}

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::put::Arg,
	) -> impl Future<Output = tg::Result<tg::build::put::Output>> {
		match self {
			Either::Left(s) => s.put_build(id, arg).left_future(),
			Either::Right(s) => s.put_build(id, arg).right_future(),
		}
	}

	fn push_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
	> {
		match self {
			Either::Left(s) => s
				.push_build(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.push_build(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn pull_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
	> {
		match self {
			Either::Left(s) => s
				.pull_build(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.pull_build(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn try_dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::build::dequeue::Output>>> {
		match self {
			Either::Left(s) => s.try_dequeue_build(arg).left_future(),
			Either::Right(s) => s.try_dequeue_build(arg).right_future(),
		}
	}

	fn try_start_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::start::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> + Send {
		match self {
			Either::Left(s) => s.try_start_build(id, arg).left_future(),
			Either::Right(s) => s.try_start_build(id, arg).right_future(),
		}
	}

	fn try_get_build_status_stream(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::status::Event>> + Send + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_build_status_stream(id)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_build_status_stream(id)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_get_build_children_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::build::children::get::Event>> + Send + 'static,
			>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_build_children_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_build_children_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn add_build_child(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.add_build_child(id, arg).left_future(),
			Either::Right(s) => s.add_build_child(id, arg).right_future(),
		}
	}

	async fn try_get_build_log_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::log::get::Event>> + Send + 'static>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_build_log_stream(id, arg)
				.await
				.map(|option| option.map(futures::StreamExt::left_stream)),
			Either::Right(s) => s
				.try_get_build_log_stream(id, arg)
				.await
				.map(|option| option.map(futures::StreamExt::right_stream)),
		}
	}

	fn add_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.add_build_log(id, arg).left_future(),
			Either::Right(s) => s.add_build_log(id, arg).right_future(),
		}
	}

	fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_build_outcome(id)
				.map(|result| result.map(|option| option.map(futures::FutureExt::left_future)))
				.left_future(),
			Either::Right(s) => s
				.try_get_build_outcome(id)
				.map(|result| result.map(|option| option.map(futures::FutureExt::right_future)))
				.right_future(),
		}
	}

	fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		match self {
			Either::Left(s) => s.finish_build(id, arg).left_future(),
			Either::Right(s) => s.finish_build(id, arg).right_future(),
		}
	}

	fn touch_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Either::Left(s) => s.touch_build(id, arg).left_future(),
			Either::Right(s) => s.touch_build(id, arg).right_future(),
		}
	}

	fn heartbeat_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::build::heartbeat::Output>> + Send {
		match self {
			Either::Left(s) => s.heartbeat_build(id, arg).left_future(),
			Either::Right(s) => s.heartbeat_build(id, arg).right_future(),
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
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
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
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
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

	fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> impl Future<Output = tg::Result<Option<tg::reference::get::Output>>> + Send {
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

	fn health(&self) -> impl Future<Output = tg::Result<tg::server::Health>> {
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

	fn try_build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::target::build::Output>>> {
		match self {
			Either::Left(s) => s.try_build_target(id, arg).left_future(),
			Either::Right(s) => s.try_build_target(id, arg).right_future(),
		}
	}

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			Either::Left(s) => s.get_user(token).left_future(),
			Either::Right(s) => s.get_user(token).right_future(),
		}
	}
}
