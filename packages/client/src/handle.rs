use crate::{
	artifact, build, directory, health, object, package, target, user, Dependency, Id, User,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use tangram_error::{Result, WrapErr};

#[async_trait]
pub trait Handle: Send + Sync + 'static {
	fn clone_box(&self) -> Box<dyn Handle>;

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore;

	async fn health(&self) -> Result<health::Health>;

	async fn clean(&self) -> Result<()>;

	async fn stop(&self) -> Result<()>;

	async fn check_in_artifact(&self, arg: artifact::CheckInArg)
		-> Result<artifact::CheckInOutput>;

	async fn check_out_artifact(&self, arg: artifact::CheckOutArg) -> Result<()>;

	async fn try_list_builds(&self, arg: build::ListArg) -> Result<build::ListOutput>;

	async fn get_build_exists(&self, id: &build::Id) -> Result<bool>;

	async fn get_build(&self, id: &build::Id) -> Result<build::GetOutput> {
		Ok(self
			.try_get_build(id)
			.await?
			.wrap_err("Failed to get the build.")?)
	}

	async fn try_get_build(&self, id: &build::Id) -> Result<Option<build::GetOutput>>;

	async fn try_put_build(
		&self,
		user: Option<&User>,
		id: &build::Id,
		state: &build::State,
	) -> Result<build::PutOutput>;

	async fn get_or_create_build(
		&self,
		user: Option<&User>,
		arg: build::GetOrCreateArg,
	) -> Result<build::GetOrCreateOutput>;

	async fn try_get_build_from_queue(
		&self,
		user: Option<&User>,
		arg: build::GetBuildFromQueueArg,
	) -> Result<Option<build::GetBuildFromQueueOutput>>;

	async fn get_build_status(&self, id: &build::Id) -> Result<build::Status> {
		Ok(self
			.try_get_build_status(id)
			.await?
			.wrap_err("Failed to get the build.")?)
	}

	async fn try_get_build_status(&self, id: &build::Id) -> Result<Option<build::Status>>;

	async fn set_build_status(
		&self,
		user: Option<&User>,
		id: &build::Id,
		status: build::Status,
	) -> Result<()>;

	async fn get_build_target(&self, id: &build::Id) -> Result<target::Id> {
		Ok(self
			.try_get_build_target(id)
			.await?
			.wrap_err("Failed to get the build.")?)
	}

	async fn try_get_build_target(&self, id: &build::Id) -> Result<Option<target::Id>>;

	async fn get_build_children(
		&self,
		id: &build::Id,
	) -> Result<BoxStream<'static, Result<build::Id>>> {
		Ok(self
			.try_get_build_children(id)
			.await?
			.wrap_err("Failed to get the build.")?)
	}

	async fn try_get_build_children(
		&self,
		id: &build::Id,
	) -> Result<Option<BoxStream<'static, Result<build::Id>>>>;

	async fn add_build_child(
		&self,
		user: Option<&User>,
		id: &build::Id,
		child_id: &build::Id,
	) -> Result<()>;

	async fn get_build_log(
		&self,
		id: &build::Id,
		arg: build::GetLogArg,
	) -> Result<BoxStream<'static, Result<build::LogEntry>>> {
		Ok(self
			.try_get_build_log(id, arg)
			.await?
			.wrap_err("Failed to get the build.")?)
	}

	async fn try_get_build_log(
		&self,
		id: &build::Id,
		arg: build::GetLogArg,
	) -> Result<Option<BoxStream<'static, Result<build::LogEntry>>>>;

	async fn add_build_log(&self, user: Option<&User>, id: &build::Id, bytes: Bytes) -> Result<()>;

	async fn get_build_outcome(&self, id: &build::Id) -> Result<build::Outcome> {
		Ok(self
			.try_get_build_outcome(id)
			.await?
			.wrap_err("Failed to get the build.")?)
	}

	async fn try_get_build_outcome(&self, id: &build::Id) -> Result<Option<build::Outcome>>;

	async fn finish_build(
		&self,
		user: Option<&User>,
		id: &build::Id,
		outcome: build::Outcome,
	) -> Result<()>;

	async fn get_object_exists(&self, id: &object::Id) -> Result<bool>;

	async fn get_object(&self, id: &object::Id) -> Result<object::GetOutput> {
		Ok(self
			.try_get_object(id)
			.await?
			.wrap_err("Failed to get the object.")?)
	}

	async fn try_get_object(&self, id: &object::Id) -> Result<Option<object::GetOutput>>;

	async fn try_put_object(&self, id: &object::Id, bytes: &Bytes) -> Result<object::PutOutput>;

	async fn push_object(&self, id: &object::Id) -> Result<()>;

	async fn pull_object(&self, id: &object::Id) -> Result<()>;

	async fn search_packages(&self, arg: package::SearchArg) -> Result<Vec<String>>;

	async fn get_package(
		&self,
		dependency: &Dependency,
		arg: package::GetArg,
	) -> Result<package::GetOutput> {
		Ok(self
			.try_get_package(dependency, arg)
			.await?
			.wrap_err("Failed to get the package.")?)
	}

	async fn try_get_package(
		&self,
		dependency: &Dependency,
		arg: package::GetArg,
	) -> Result<Option<package::GetOutput>>;

	async fn get_package_versions(&self, dependency: &Dependency) -> Result<Vec<String>> {
		Ok(self
			.try_get_package_versions(dependency)
			.await?
			.wrap_err("Failed to get the package versions.")?)
	}

	async fn try_get_package_versions(
		&self,
		dependency: &Dependency,
	) -> Result<Option<Vec<String>>>;

	async fn get_package_metadata(&self, dependency: &Dependency) -> Result<package::Metadata> {
		Ok(self
			.try_get_package_metadata(dependency)
			.await?
			.wrap_err("Failed to get the package metadata.")?)
	}

	async fn try_get_package_metadata(
		&self,
		dependency: &Dependency,
	) -> Result<Option<package::Metadata>>;

	async fn get_package_dependencies(&self, dependency: &Dependency) -> Result<Vec<Dependency>> {
		Ok(self
			.try_get_package_dependencies(dependency)
			.await?
			.wrap_err("Failed to get the package dependencies.")?)
	}

	async fn try_get_package_dependencies(
		&self,
		dependency: &Dependency,
	) -> Result<Option<Vec<Dependency>>>;

	async fn publish_package(&self, user: Option<&User>, id: &directory::Id) -> Result<()>;

	async fn create_login(&self) -> Result<user::Login>;

	async fn get_login(&self, id: &Id) -> Result<Option<user::Login>>;

	async fn get_user_for_token(&self, token: &str) -> Result<Option<User>>;
}
