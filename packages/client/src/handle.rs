use crate as tg;
use bytes::Bytes;
use futures::{Future, FutureExt, Stream};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

mod either;

pub trait Handle: Clone + Unpin + Send + Sync + 'static {
	type Transaction<'a>: Send + Sync;

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::archive::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::archive::Output>> + Send;

	fn extract_artifact(
		&self,
		arg: tg::artifact::extract::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::extract::Output>> + Send;

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::bundle::Output>> + Send;

	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkin::Output>> + Send;

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkout::Output>> + Send;

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> + Send;

	fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::compress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::compress::Output>> + Send;

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::decompress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::decompress::Output>> + Send;

	fn list_builds(
		&self,
		arg: tg::build::list::Arg,
	) -> impl Future<Output = tg::Result<tg::build::list::Output>> + Send;

	fn get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<tg::build::get::Output>> + Send {
		self.try_get_build(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<tg::build::get::Output>>> + Send;

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn push_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn pull_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn create_build(
		&self,
		arg: tg::build::create::Arg,
	) -> impl Future<Output = tg::Result<tg::build::create::Output>> + Send;

	fn try_dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<tg::build::dequeue::Output>>> + Send;

	fn try_start_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<bool>>> + Send;

	fn get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
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
		arg: tg::build::status::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
		>,
	> + Send;

	fn get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
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
		arg: tg::build::children::Arg,
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
		arg: tg::build::log::Arg,
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
		arg: tg::build::log::Arg,
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
		arg: tg::build::outcome::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send {
		self.try_get_build_outcome(id, arg, stop).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<Option<tg::build::Outcome>>>> + Send;

	fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn touch_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn format(&self, text: String) -> impl Future<Output = tg::Result<String>> + Send;

	fn lsp(
		&self,
		input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::get::Output>> + Send {
		self.try_get_object(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the object")))
		})
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> + Send;

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> + Send;

	fn push_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn pull_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> impl Future<Output = tg::Result<tg::package::list::Output>> + Send;

	fn get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> impl Future<Output = tg::Result<tg::package::get::Output>> + Send {
		self.try_get_package(dependency, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the package")))
		})
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::package::get::Output>>> + Send;

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
	) -> impl Future<Output = tg::Result<tg::package::outdated::Output>> + Send;

	fn publish_package(
		&self,
		id: &tg::directory::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<tg::package::versions::Output>> + Send {
		self.try_get_package_versions(dependency).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the package")))
		})
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> + Send;

	fn yank_package(&self, id: &tg::directory::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn list_roots(
		&self,
		arg: tg::root::list::Arg,
	) -> impl Future<Output = tg::Result<tg::root::list::Output>> + Send;

	fn try_get_root(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::root::get::Output>>> + Send;

	fn put_root(&self, arg: tg::root::add::Arg) -> impl Future<Output = tg::Result<()>> + Send;

	fn remove_root(&self, name: &str) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_js_runtime_doc(&self) -> impl Future<Output = tg::Result<serde_json::Value>> + Send;

	fn health(&self) -> impl Future<Output = tg::Result<tg::server::Health>> + Send;

	fn clean(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn stop(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> + Send;
}
