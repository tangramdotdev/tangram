use crate as tg;
use bytes::Bytes;
use either::Either;
use futures::{Future, FutureExt as _, Stream};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

impl<L, R> tg::Handle for Either<L, R>
where
	L: tg::Handle,
	R: tg::Handle,
{
	type Transaction<'a> = Either<L::Transaction<'a>, R::Transaction<'a>>;

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::archive::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::archive::Output>> {
		match self {
			Either::Left(s) => s.archive_artifact(id, arg).left_future(),
			Either::Right(s) => s.archive_artifact(id, arg).right_future(),
		}
	}

	fn extract_artifact(
		&self,
		arg: tg::artifact::extract::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::extract::Output>> {
		match self {
			Either::Left(s) => s.extract_artifact(arg).left_future(),
			Either::Right(s) => s.extract_artifact(arg).right_future(),
		}
	}

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::bundle::Output>> {
		match self {
			Either::Left(s) => s.bundle_artifact(id).left_future(),
			Either::Right(s) => s.bundle_artifact(id).right_future(),
		}
	}

	fn checksum_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checksum::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checksum::Output>> {
		match self {
			Either::Left(s) => s.checksum_artifact(id, arg).left_future(),
			Either::Right(s) => s.checksum_artifact(id, arg).right_future(),
		}
	}

	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkin::Output>> {
		match self {
			Either::Left(s) => s.check_in_artifact(arg).left_future(),
			Either::Right(s) => s.check_in_artifact(arg).right_future(),
		}
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkout::Output>> {
		match self {
			Either::Left(s) => s.check_out_artifact(id, arg).left_future(),
			Either::Right(s) => s.check_out_artifact(id, arg).right_future(),
		}
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> {
		match self {
			Either::Left(s) => {
				let transaction = transaction.map(|t| t.as_ref().left().unwrap());
				s.create_blob(reader, transaction).left_future()
			},
			Either::Right(s) => {
				let transaction = transaction.map(|t| t.as_ref().right().unwrap());
				s.create_blob(reader, transaction).right_future()
			},
		}
	}

	fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::compress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::compress::Output>> {
		match self {
			Either::Left(s) => s.compress_blob(id, arg).left_future(),
			Either::Right(s) => s.compress_blob(id, arg).right_future(),
		}
	}

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::decompress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::decompress::Output>> {
		match self {
			Either::Left(s) => s.decompress_blob(id, arg).left_future(),
			Either::Right(s) => s.decompress_blob(id, arg).right_future(),
		}
	}

	fn download_blob(
		&self,
		arg: tg::blob::download::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::download::Output>> {
		match self {
			Either::Left(s) => s.download_blob(arg).left_future(),
			Either::Right(s) => s.download_blob(arg).right_future(),
		}
	}

	fn checksum_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::checksum::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::checksum::Output>> {
		match self {
			Either::Left(s) => s.checksum_blob(id, arg).left_future(),
			Either::Right(s) => s.checksum_blob(id, arg).right_future(),
		}
	}

	fn list_builds(
		&self,
		arg: tg::build::list::Arg,
	) -> impl Future<Output = tg::Result<tg::build::list::Output>> {
		match self {
			Either::Left(s) => s.list_builds(arg).left_future(),
			Either::Right(s) => s.list_builds(arg).right_future(),
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
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.put_build(id, arg).left_future(),
			Either::Right(s) => s.put_build(id, arg).right_future(),
		}
	}

	fn push_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.push_build(id).left_future(),
			Either::Right(s) => s.push_build(id).right_future(),
		}
	}

	fn pull_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.pull_build(id).left_future(),
			Either::Right(s) => s.pull_build(id).right_future(),
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
	) -> impl Future<Output = tg::Result<Option<bool>>> + Send {
		match self {
			Either::Left(s) => s.try_start_build(id).left_future(),
			Either::Right(s) => s.try_start_build(id).right_future(),
		}
	}

	fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_build_status(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_build_status(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_build_children(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_build_children(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn add_build_child(
		&self,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.add_build_child(build_id, child_id).left_future(),
			Either::Right(s) => s.add_build_child(build_id, child_id).right_future(),
		}
	}

	async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>>
	{
		match self {
			Either::Left(s) => s
				.try_get_build_log(id, arg)
				.await
				.map(|option| option.map(futures::StreamExt::left_stream)),
			Either::Right(s) => s
				.try_get_build_log(id, arg)
				.await
				.map(|option| option.map(futures::StreamExt::right_stream)),
		}
	}

	fn add_build_log(
		&self,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.add_build_log(id, bytes).left_future(),
			Either::Right(s) => s.add_build_log(id, bytes).right_future(),
		}
	}

	fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
	) -> impl Future<Output = tg::Result<Option<Option<tg::build::Outcome>>>> {
		match self {
			Either::Left(s) => s.try_get_build_outcome(id, arg).left_future(),
			Either::Right(s) => s.try_get_build_outcome(id, arg).right_future(),
		}
	}

	fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.finish_build(id, arg).left_future(),
			Either::Right(s) => s.finish_build(id, arg).right_future(),
		}
	}

	fn touch_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Either::Left(s) => s.touch_build(id).left_future(),
			Either::Right(s) => s.touch_build(id).right_future(),
		}
	}

	fn heartbeat_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<tg::build::heartbeat::Output>> + Send {
		match self {
			Either::Left(s) => s.heartbeat_build(id).left_future(),
			Either::Right(s) => s.heartbeat_build(id).right_future(),
		}
	}

	fn format(&self, text: String) -> impl Future<Output = tg::Result<String>> {
		match self {
			Either::Left(s) => s.format(text).left_future(),
			Either::Right(s) => s.format(text).right_future(),
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
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		match self {
			Either::Left(s) => {
				let transaction = transaction.map(|t| t.as_ref().left().unwrap());
				s.put_object(id, arg, transaction).left_future()
			},
			Either::Right(s) => {
				let transaction = transaction.map(|t| t.as_ref().right().unwrap());
				s.put_object(id, arg, transaction).right_future()
			},
		}
	}

	fn push_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.push_object(id).left_future(),
			Either::Right(s) => s.push_object(id).right_future(),
		}
	}

	fn pull_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.pull_object(id).left_future(),
			Either::Right(s) => s.pull_object(id).right_future(),
		}
	}

	fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> impl Future<Output = tg::Result<tg::package::list::Output>> {
		match self {
			Either::Left(s) => s.list_packages(arg).left_future(),
			Either::Right(s) => s.list_packages(arg).right_future(),
		}
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::package::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_package(dependency, arg).left_future(),
			Either::Right(s) => s.try_get_package(dependency, arg).right_future(),
		}
	}

	fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Vec<tg::Diagnostic>>> {
		match self {
			Either::Left(s) => s.check_package(dependency).left_future(),
			Either::Right(s) => s.check_package(dependency).right_future(),
		}
	}

	fn format_package(&self, dependency: &tg::Dependency) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.format_package(dependency).left_future(),
			Either::Right(s) => s.format_package(dependency).right_future(),
		}
	}

	fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<serde_json::Value>>> {
		match self {
			Either::Left(s) => s.try_get_package_doc(dependency).left_future(),
			Either::Right(s) => s.try_get_package_doc(dependency).right_future(),
		}
	}

	fn get_package_outdated(
		&self,
		arg: &tg::Dependency,
	) -> impl Future<Output = tg::Result<tg::package::outdated::Output>> {
		match self {
			Either::Left(s) => s.get_package_outdated(arg).left_future(),
			Either::Right(s) => s.get_package_outdated(arg).right_future(),
		}
	}

	fn publish_package(&self, id: &tg::directory::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.publish_package(id).left_future(),
			Either::Right(s) => s.publish_package(id).right_future(),
		}
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> {
		match self {
			Either::Left(s) => s.try_get_package_versions(dependency).left_future(),
			Either::Right(s) => s.try_get_package_versions(dependency).right_future(),
		}
	}

	fn yank_package(&self, id: &tg::directory::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.yank_package(id).left_future(),
			Either::Right(s) => s.yank_package(id).right_future(),
		}
	}

	fn list_roots(
		&self,
		arg: tg::root::list::Arg,
	) -> impl Future<Output = tg::Result<tg::root::list::Output>> {
		match self {
			Either::Left(s) => s.list_roots(arg).left_future(),
			Either::Right(s) => s.list_roots(arg).right_future(),
		}
	}

	fn try_get_root(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::root::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_root(name).left_future(),
			Either::Right(s) => s.try_get_root(name).right_future(),
		}
	}

	fn put_root(
		&self,
		name: &str,
		arg: tg::root::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.put_root(name, arg).left_future(),
			Either::Right(s) => s.put_root(name, arg).right_future(),
		}
	}

	fn delete_root(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.delete_root(name).left_future(),
			Either::Right(s) => s.delete_root(name).right_future(),
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

	fn build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<tg::target::build::Output>> {
		match self {
			Either::Left(s) => s.build_target(id, arg).left_future(),
			Either::Right(s) => s.build_target(id, arg).right_future(),
		}
	}

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			Either::Left(s) => s.get_user(token).left_future(),
			Either::Right(s) => s.get_user(token).right_future(),
		}
	}
}
