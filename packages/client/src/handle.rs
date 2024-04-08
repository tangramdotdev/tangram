use crate::{self as tg, error};
use bytes::Bytes;
use futures::{Future, FutureExt, Stream};
use tokio::io::{AsyncRead, AsyncWrite};

pub trait Handle: Clone + Unpin + Send + Sync + 'static {
	fn path(&self) -> impl Future<Output = tg::Result<Option<tg::Path>>> + Send;

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore;

	fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckInOutput>> + Send;

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckOutOutput>> + Send;

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
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the build")))
		})
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::build::GetOutput>>> + Send;

	fn put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn push_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn pull_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> impl Future<Output = tg::Result<tg::build::GetOrCreateOutput>> + Send;

	fn get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
	> + Send {
		self.try_get_build_status(id, arg, stop).map(|result| {
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the build")))
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
		user: Option<&tg::User>,
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
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the build")))
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
		user: Option<&tg::User>,
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
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the build")))
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
		user: Option<&tg::User>,
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
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the build")))
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
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn format(&self, text: String) -> impl Future<Output = tg::Result<String>> + Send;

	fn lsp(
		&self,
		input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::GetOutput>> + Send {
		self.try_get_object(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the object")))
		})
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::GetOutput>>> + Send;

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
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
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the package")))
		})
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::package::GetOutput>>> + Send;

	fn get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Vec<String>>> + Send {
		self.try_get_package_versions(dependency).map(|result| {
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the package")))
		})
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> + Send;

	fn publish_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Vec<tg::Diagnostic>>> + Send;

	fn yank_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn format_package(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<tg::package::OutdatedOutput>> + Send;

	fn get_runtime_doc(&self) -> impl Future<Output = tg::Result<serde_json::Value>> + Send;

	fn get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<serde_json::Value>> + Send {
		self.try_get_package_doc(dependency).map(|result| {
			result.and_then(|option| option.ok_or_else(|| error!("failed to get the package")))
		})
	}

	fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<serde_json::Value>>> + Send;

	fn health(&self) -> impl Future<Output = tg::Result<tg::server::Health>> + Send;

	fn clean(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn stop(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn create_login(&self) -> impl Future<Output = tg::Result<tg::user::Login>> + Send;

	fn get_login(
		&self,
		id: &tg::Id,
	) -> impl Future<Output = tg::Result<Option<tg::user::Login>>> + Send;

	fn get_user_for_token(
		&self,
		token: &str,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> + Send;
}
