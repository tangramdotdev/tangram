use crate as tg;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tangram_error::{error, Result};
use tokio::io::{AsyncRead, AsyncWrite};
use url::Url;

#[async_trait]
pub trait Handle: Send + Sync + 'static {
	fn clone_box(&self) -> Box<dyn Handle>;

	async fn path(&self) -> Result<Option<tg::Path>>;

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore;

	async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> Result<tg::artifact::CheckInOutput>;

	async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> Result<tg::artifact::CheckOutOutput>;

	async fn list_builds(&self, arg: tg::build::ListArg) -> Result<tg::build::ListOutput>;

	async fn get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> Result<tg::build::GetOutput> {
		Ok(self
			.try_get_build(id, arg)
			.await?
			.ok_or_else(|| error!("failed to get the build"))?)
	}

	async fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> Result<Option<tg::build::GetOutput>>;

	async fn put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> Result<()>;

	async fn push_build(&self, user: Option<&tg::User>, id: &tg::build::Id) -> Result<()>;

	async fn pull_build(&self, id: &tg::build::Id) -> Result<()>;

	async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput>;

	async fn get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<BoxStream<'static, Result<tg::build::Status>>> {
		Ok(self
			.try_get_build_status(id, arg, stop)
			.await?
			.ok_or_else(|| error!("failed to get the build"))?)
	}

	async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>>;

	async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()>;

	async fn get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<BoxStream<'static, Result<tg::build::children::Chunk>>> {
		Ok(self
			.try_get_build_children(id, arg, stop)
			.await?
			.ok_or_else(|| error!("failed to get the build"))?)
	}

	async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::children::Chunk>>>>;

	async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<()>;

	async fn get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<BoxStream<'static, Result<tg::build::log::Chunk>>> {
		Ok(self
			.try_get_build_log(id, arg, stop)
			.await?
			.ok_or_else(|| error!("failed to get the build"))?)
	}

	async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>>;

	async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()>;

	async fn get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<tg::build::Outcome>> {
		Ok(self
			.try_get_build_outcome(id, arg, stop)
			.await?
			.ok_or_else(|| error!("failed to get the build"))?)
	}

	async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>>;

	async fn set_build_outcome(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()>;

	async fn format(&self, text: String) -> Result<String>;

	async fn lsp(
		&self,
		input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> Result<()>;

	async fn get_object(&self, id: &tg::object::Id) -> Result<tg::object::GetOutput> {
		Ok(self
			.try_get_object(id)
			.await?
			.ok_or_else(|| error!("failed to get the object"))?)
	}

	async fn try_get_object(&self, id: &tg::object::Id) -> Result<Option<tg::object::GetOutput>>;

	async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> Result<tg::object::PutOutput>;

	async fn push_object(&self, id: &tg::object::Id) -> Result<()>;

	async fn pull_object(&self, id: &tg::object::Id) -> Result<()>;

	async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> Result<tg::package::SearchOutput>;

	async fn get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<tg::package::GetOutput> {
		Ok(self
			.try_get_package(dependency, arg)
			.await?
			.ok_or_else(|| error!("failed to get the package"))?)
	}

	async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>>;

	async fn get_package_versions(&self, dependency: &tg::Dependency) -> Result<Vec<String>> {
		Ok(self
			.try_get_package_versions(dependency)
			.await?
			.ok_or_else(|| error!("failed to get the package"))?)
	}

	async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>>;

	async fn publish_package(&self, user: Option<&tg::User>, id: &tg::directory::Id) -> Result<()>;

	async fn check_package(&self, dependency: &tg::Dependency) -> Result<Vec<tg::Diagnostic>>;

	async fn format_package(&self, dependency: &tg::Dependency) -> Result<()>;

	async fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
	) -> Result<tg::package::OutdatedOutput>;

	async fn get_runtime_doc(&self) -> Result<serde_json::Value>;

	async fn get_package_doc(&self, dependency: &tg::Dependency) -> Result<serde_json::Value> {
		Ok(self
			.try_get_package_doc(dependency)
			.await?
			.ok_or_else(|| error!("failed to get the package"))?)
	}

	async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<serde_json::Value>>;

	async fn health(&self) -> Result<tg::server::Health>;

	async fn clean(&self) -> Result<()>;

	async fn stop(&self) -> Result<()>;

	async fn create_login(&self) -> Result<tg::user::Login>;

	async fn get_login(&self, id: &tg::Id) -> Result<Option<tg::user::Login>>;

	async fn get_user_for_token(&self, token: &str) -> Result<Option<tg::User>>;

	async fn create_oauth_url(&self, id: &tg::Id) -> Result<Url>;

	async fn complete_login(&self, id: &tg::Id, code: String) -> Result<()>;
}
