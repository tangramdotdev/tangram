use crate as tg;
use bytes::Bytes;
use either::Either;
use futures::{Future, FutureExt, Stream};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

pub trait Handle: Clone + Unpin + Send + Sync + 'static {
	type Transaction<'a>: Send + Sync;

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::ArchiveArg,
	) -> impl Future<Output = tg::Result<tg::artifact::ArchiveOutput>> + Send;

	fn extract_artifact(
		&self,
		arg: tg::artifact::ExtractArg,
	) -> impl Future<Output = tg::Result<tg::artifact::ExtractOutput>> + Send;

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::BundleOutput>> + Send;

	fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckInOutput>> + Send;

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckOutOutput>> + Send;

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> + Send;

	fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::CompressArg,
	) -> impl Future<Output = tg::Result<tg::blob::CompressOutput>> + Send;

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::DecompressArg,
	) -> impl Future<Output = tg::Result<tg::blob::DecompressOutput>> + Send;

	fn list_builds(
		&self,
		arg: tg::build::ListArg,
	) -> impl Future<Output = tg::Result<tg::build::ListOutput>> + Send;

	fn get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> impl Future<Output = tg::Result<tg::build::GetOutput>> + Send {
		self.try_get_build(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::build::GetOutput>>> + Send;

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn push_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn pull_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_or_create_build(
		&self,
		arg: tg::build::GetOrCreateArg,
	) -> impl Future<Output = tg::Result<tg::build::GetOrCreateOutput>> + Send;

	fn try_dequeue_build(
		&self,
		arg: tg::build::DequeueArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<tg::build::DequeueOutput>>> + Send;

	fn get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
	> + Send {
		self.try_get_build_status(id, arg, stop).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
		>,
	> + Send;

	fn set_build_status(
		&self,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static,
		>,
	> + Send {
		self.try_get_build_children(id, arg, stop).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
		>,
	> + Send;

	fn add_build_child(
		&self,
		id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>,
	> + Send {
		self.try_get_build_log(id, arg, stop).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>,
		>,
	> + Send;

	fn add_build_log(
		&self,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send {
		self.try_get_build_outcome(id, arg, stop).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<Option<tg::build::Outcome>>>> + Send;

	fn set_build_outcome(
		&self,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn format(&self, text: String) -> impl Future<Output = tg::Result<String>> + Send;

	fn lsp(
		&self,
		input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::GetOutput>> + Send {
		self.try_get_object(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the object")))
		})
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::GetOutput>>> + Send;

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::PutArg,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::PutOutput>> + Send;

	fn push_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn pull_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> impl Future<Output = tg::Result<tg::package::SearchOutput>> + Send;

	fn get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> impl Future<Output = tg::Result<tg::package::GetOutput>> + Send {
		self.try_get_package(dependency, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the package")))
		})
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::package::GetOutput>>> + Send;

	fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Vec<tg::Diagnostic>>> + Send;

	fn get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<serde_json::Value>> + Send {
		self.try_get_package_doc(dependency).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the package")))
		})
	}

	fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<serde_json::Value>>> + Send;

	fn format_package(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<tg::package::OutdatedOutput>> + Send;

	fn publish_package(
		&self,
		id: &tg::directory::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Vec<String>>> + Send {
		self.try_get_package_versions(dependency).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the package")))
		})
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> + Send;

	fn yank_package(&self, id: &tg::directory::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_js_runtime_doc(&self) -> impl Future<Output = tg::Result<serde_json::Value>> + Send;

	fn health(&self) -> impl Future<Output = tg::Result<tg::server::Health>> + Send;

	fn clean(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn stop(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> + Send;
}

impl<L, R> Handle for Either<L, R>
where
	L: Handle,
	R: Handle,
{
	type Transaction<'a> = Either<L::Transaction<'a>, R::Transaction<'a>>;

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::ArchiveArg,
	) -> impl Future<Output = tg::Result<tg::artifact::ArchiveOutput>> {
		match self {
			Either::Left(s) => s.archive_artifact(id, arg).left_future(),
			Either::Right(s) => s.archive_artifact(id, arg).right_future(),
		}
	}

	fn extract_artifact(
		&self,
		arg: tg::artifact::ExtractArg,
	) -> impl Future<Output = tg::Result<tg::artifact::ExtractOutput>> {
		match self {
			Either::Left(s) => s.extract_artifact(arg).left_future(),
			Either::Right(s) => s.extract_artifact(arg).right_future(),
		}
	}

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::BundleOutput>> {
		match self {
			Either::Left(s) => s.bundle_artifact(id).left_future(),
			Either::Right(s) => s.bundle_artifact(id).right_future(),
		}
	}

	fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckInOutput>> {
		match self {
			Either::Left(s) => s.check_in_artifact(arg).left_future(),
			Either::Right(s) => s.check_in_artifact(arg).right_future(),
		}
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckOutOutput>> {
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
		arg: tg::blob::CompressArg,
	) -> impl Future<Output = tg::Result<tg::blob::CompressOutput>> {
		match self {
			Either::Left(s) => s.compress_blob(id, arg).left_future(),
			Either::Right(s) => s.compress_blob(id, arg).right_future(),
		}
	}

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::DecompressArg,
	) -> impl Future<Output = tg::Result<tg::blob::DecompressOutput>> {
		match self {
			Either::Left(s) => s.decompress_blob(id, arg).left_future(),
			Either::Right(s) => s.decompress_blob(id, arg).right_future(),
		}
	}

	fn list_builds(
		&self,
		arg: tg::build::ListArg,
	) -> impl Future<Output = tg::Result<tg::build::ListOutput>> {
		match self {
			Either::Left(s) => s.list_builds(arg).left_future(),
			Either::Right(s) => s.list_builds(arg).right_future(),
		}
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::build::GetOutput>>> {
		match self {
			Either::Left(s) => s.try_get_build(id, arg).left_future(),
			Either::Right(s) => s.try_get_build(id, arg).right_future(),
		}
	}

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
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

	fn get_or_create_build(
		&self,
		arg: tg::build::GetOrCreateArg,
	) -> impl Future<Output = tg::Result<tg::build::GetOrCreateOutput>> {
		match self {
			Either::Left(s) => s.get_or_create_build(arg).left_future(),
			Either::Right(s) => s.get_or_create_build(arg).right_future(),
		}
	}

	fn try_dequeue_build(
		&self,
		arg: tg::build::DequeueArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<tg::build::DequeueOutput>>> {
		match self {
			Either::Left(s) => s.try_dequeue_build(arg, stop).left_future(),
			Either::Right(s) => s.try_dequeue_build(arg, stop).right_future(),
		}
	}

	fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_build_status(id, arg, stop)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_build_status(id, arg, stop)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn set_build_status(
		&self,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.set_build_status(id, status).left_future(),
			Either::Right(s) => s.set_build_status(id, status).right_future(),
		}
	}

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_build_children(id, arg, stop)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_build_children(id, arg, stop)
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
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>>
	{
		match self {
			Either::Left(s) => s
				.try_get_build_log(id, arg, stop)
				.await
				.map(|option| option.map(futures::StreamExt::left_stream)),
			Either::Right(s) => s
				.try_get_build_log(id, arg, stop)
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
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<Option<tg::build::Outcome>>>> {
		match self {
			Either::Left(s) => s.try_get_build_outcome(id, arg, stop).left_future(),
			Either::Right(s) => s.try_get_build_outcome(id, arg, stop).right_future(),
		}
	}

	fn set_build_outcome(
		&self,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.set_build_outcome(id, outcome).left_future(),
			Either::Right(s) => s.set_build_outcome(id, outcome).right_future(),
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
		input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.lsp(input, output).left_future(),
			Either::Right(s) => s.lsp(input, output).right_future(),
		}
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::GetOutput>>> {
		match self {
			Either::Left(s) => s.try_get_object(id).left_future(),
			Either::Right(s) => s.try_get_object(id).right_future(),
		}
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::PutArg,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::PutOutput>> {
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

	fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> impl Future<Output = tg::Result<tg::package::SearchOutput>> {
		match self {
			Either::Left(s) => s.search_packages(arg).left_future(),
			Either::Right(s) => s.search_packages(arg).right_future(),
		}
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::package::GetOutput>>> {
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
	) -> impl Future<Output = tg::Result<tg::package::OutdatedOutput>> {
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

	fn stop(&self) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.stop().left_future(),
			Either::Right(s) => s.stop().right_future(),
		}
	}

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			Either::Left(s) => s.get_user(token).left_future(),
			Either::Right(s) => s.get_user(token).right_future(),
		}
	}
}
