use bytes::Bytes;
use futures::{Future, Stream};
use std::sync::Arc;
use tangram_client as tg;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

#[derive(Clone)]
pub struct Server(Arc<Inner>);

pub struct Inner {
	build: tg::build::Id,
	path_map: Option<PathMap>,
	server: crate::Server,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PathMap {
	pub output_guest: tg::Path,
	pub output_host: tg::Path,
	pub root_host: tg::Path,
}

impl Server {
	pub fn new(server: crate::Server, build: tg::build::Id, path_map: Option<PathMap>) -> Self {
		let inner = Inner {
			build,
			path_map,
			server,
		};
		Self(Arc::new(inner))
	}

	fn host_path_for_guest_path(&self, path: tg::Path) -> tg::Result<tg::Path> {
		// Get the path map. If there is no path map, then the guest path is the host path.
		let Some(path_map) = &self.path_map else {
			return Ok(path);
		};

		// Map the path.
		if let Some(path) = path
			.diff(&path_map.output_guest)
			.filter(tg::Path::is_internal)
		{
			Ok(path_map.output_host.clone().join(path))
		} else {
			let path = path
				.diff(&"/".parse().unwrap())
				.ok_or_else(|| tg::error!("the path must be absolute"))?;
			Ok(path_map.root_host.clone().join(path))
		}
	}
}

impl tg::Handle for Server {
	type Transaction<'a> = ();

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::archive::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::archive::Output>> {
		self.server.archive_artifact(id, arg)
	}

	fn extract_artifact(
		&self,
		arg: tg::artifact::extract::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::extract::Output>> {
		self.server.extract_artifact(arg)
	}

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::bundle::Output>> {
		self.server.bundle_artifact(id)
	}

	fn checksum_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checksum::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checksum::Output>> {
		self.server.checksum_artifact(id, arg)
	}

	async fn check_in_artifact(
		&self,
		mut arg: tg::artifact::checkin::Arg,
	) -> tg::Result<tg::artifact::checkin::Output> {
		// Replace the path with the host path.
		arg.path = self.host_path_for_guest_path(arg.path)?;

		// Perform the checkin.
		let output = self.server.check_in_artifact(arg).await?;

		// If the VFS is disabled, then check out the artifact.
		if !self.server.options.vfs {
			let arg = tg::artifact::checkout::Arg::default();
			self.check_out_artifact(&output.artifact, arg).await?;
		}

		Ok(output)
	}

	async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		mut arg: tg::artifact::checkout::Arg,
	) -> tg::Result<tg::artifact::checkout::Output> {
		// Replace the path with the host path.
		if let Some(path) = &mut arg.path {
			*path = self.host_path_for_guest_path(path.clone())?;
		}

		// Perform the checkout.
		self.server.check_out_artifact(id, arg).await
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		_transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> {
		self.server.create_blob(reader, None)
	}

	fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::compress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::compress::Output>> {
		self.server.compress_blob(id, arg)
	}

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::decompress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::decompress::Output>> {
		self.server.decompress_blob(id, arg)
	}

	fn download_blob(
		&self,
		arg: tg::blob::download::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::download::Output>> {
		self.server.download_blob(arg)
	}

	fn checksum_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::checksum::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::checksum::Output>> {
		self.server.checksum_blob(id, arg)
	}

	async fn list_builds(&self, _arg: tg::build::list::Arg) -> tg::Result<tg::build::list::Output> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<tg::build::get::Output>>> {
		self.server.try_get_build(id)
	}

	async fn put_build(&self, _id: &tg::build::Id, _arg: tg::build::put::Arg) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn push_build(&self, _id: &tg::build::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn pull_build(&self, _id: &tg::build::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_dequeue_build(
		&self,
		_arg: tg::build::dequeue::Arg,
	) -> tg::Result<Option<tg::build::dequeue::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn try_start_build(&self, _id: &tg::build::Id) -> tg::Result<Option<bool>> {
		Err(tg::error!("forbidden"))
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
		self.server.try_get_build_status(id, arg)
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
		self.server.try_get_build_children(id, arg)
	}

	async fn add_build_child(
		&self,
		_build_id: &tg::build::Id,
		_child_id: &tg::build::Id,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>,
		>,
	> {
		self.server.try_get_build_log(id, arg)
	}

	async fn add_build_log(&self, _build_id: &tg::build::Id, _bytes: Bytes) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
	) -> impl Future<Output = tg::Result<Option<Option<tg::build::Outcome>>>> {
		self.server.try_get_build_outcome(id, arg)
	}

	async fn finish_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::finish::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn touch_build(&self, _id: &tg::build::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn format(&self, _text: String) -> tg::Result<String> {
		Err(tg::error!("forbidden"))
	}

	async fn lsp(
		&self,
		_input: impl AsyncBufRead + Send + Unpin + 'static,
		_output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		self.server.try_get_object(id)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		_transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		self.server.put_object(id, arg, None)
	}

	async fn push_object(&self, _id: &tg::object::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn pull_object(&self, _id: &tg::object::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn list_packages(
		&self,
		_arg: tg::package::list::Arg,
	) -> tg::Result<tg::package::list::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::get::Arg,
	) -> tg::Result<Option<tg::package::get::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn check_package(&self, _dependency: &tg::Dependency) -> tg::Result<Vec<tg::Diagnostic>> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package_doc(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<Option<serde_json::Value>> {
		Err(tg::error!("forbidden"))
	}

	async fn format_package(&self, _dependency: &tg::Dependency) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn get_package_outdated(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<tg::package::outdated::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn publish_package(&self, _id: &tg::directory::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package_versions(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<Option<Vec<String>>> {
		Err(tg::error!("forbidden"))
	}

	async fn yank_package(&self, _id: &tg::directory::Id) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn list_roots(&self, _arg: tg::root::list::Arg) -> tg::Result<tg::root::list::Output> {
		Err(tg::error!("not supported"))
	}

	async fn try_get_root(&self, _name: &str) -> tg::Result<Option<tg::root::get::Output>> {
		Err(tg::error!("not supported"))
	}

	async fn put_root(&self, _name: &str, _arg: tg::root::put::Arg) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn delete_root(&self, _name: &str) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn get_js_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		Err(tg::error!("forbidden"))
	}

	async fn health(&self) -> tg::Result<tg::server::Health> {
		Err(tg::error!("forbidden"))
	}

	async fn clean(&self) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn build_target(
		&self,
		id: &tg::target::Id,
		mut arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<tg::target::build::Output>> {
		arg.parent = Some(self.build.clone());
		self.server.build_target(id, arg)
	}

	async fn get_user(&self, _token: &str) -> tg::Result<Option<tg::User>> {
		Err(tg::error!("forbidden"))
	}
}

impl std::ops::Deref for Server {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
