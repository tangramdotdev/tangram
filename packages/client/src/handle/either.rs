use crate as tg;
use either::Either;
use futures::{Future, FutureExt as _, Stream};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

impl<L, R> tg::Handle for Either<L, R>
where
	L: tg::Handle,
	R: tg::Handle,
{
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
	) -> impl Future<Output = tg::Result<tg::blob::create::Output>> {
		match self {
			Either::Left(s) => s.create_blob(reader).left_future(),
			Either::Right(s) => s.create_blob(reader).right_future(),
		}
	}

	fn read_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> impl Future<Output = crate::Result<Bytes>> + Send {
		match self {
			Either::Left(s) => s.read_blob(id, arg).left_future(),
			Either::Right(s) => s.read_blob(id, arg).right_future(),
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
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::build::push::Event>> + Send + 'static,
		>,
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
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::build::pull::Event>> + Send + 'static,
		>,
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

	fn start_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::start::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Either::Left(s) => s.start_build(id, arg).left_future(),
			Either::Right(s) => s.start_build(id, arg).right_future(),
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
	) -> impl Future<Output = tg::Result<()>> {
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
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::object::push::Event>> + Send + 'static,
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
			impl Stream<Item = tg::Result<tg::object::pull::Event>> + Send + 'static,
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
		arg: tg::package::check::Arg,
	) -> impl Future<Output = tg::Result<Vec<tg::Diagnostic>>> {
		match self {
			Either::Left(s) => s.check_package(dependency, arg).left_future(),
			Either::Right(s) => s.check_package(dependency, arg).right_future(),
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
		arg: tg::package::doc::Arg,
	) -> impl Future<Output = tg::Result<Option<serde_json::Value>>> {
		match self {
			Either::Left(s) => s.try_get_package_doc(dependency, arg).left_future(),
			Either::Right(s) => s.try_get_package_doc(dependency, arg).right_future(),
		}
	}

	fn get_package_outdated(
		&self,
		package: &tg::Dependency,
		arg: tg::package::outdated::Arg,
	) -> impl Future<Output = tg::Result<tg::package::outdated::Output>> {
		match self {
			Either::Left(s) => s.get_package_outdated(package, arg).left_future(),
			Either::Right(s) => s.get_package_outdated(package, arg).right_future(),
		}
	}

	fn publish_package(
		&self,
		id: &tg::artifact::Id,
		arg: tg::package::publish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.publish_package(id, arg).left_future(),
			Either::Right(s) => s.publish_package(id, arg).right_future(),
		}
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::versions::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::package::versions::Output>>> {
		match self {
			Either::Left(s) => s.try_get_package_versions(dependency, arg).left_future(),
			Either::Right(s) => s.try_get_package_versions(dependency, arg).right_future(),
		}
	}

	fn yank_package(
		&self,
		id: &tg::artifact::Id,
		arg: tg::package::yank::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.yank_package(id, arg).left_future(),
			Either::Right(s) => s.yank_package(id, arg).right_future(),
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
