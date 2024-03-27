use crate::{
	database::Transaction,
	util::http::{empty, full, Outgoing},
	Http, Server,
};
use hyper::body::Incoming;
use indoc::formatdoc;
use notify::Watcher;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

#[derive(Clone)]
pub struct FileSystemMonitor {
	inner: Arc<Inner>,
}

struct Inner {
	paths: std::sync::RwLock<lru::LruCache<tg::Path, (), fnv::FnvBuildHasher>>,
	watcher: std::sync::Mutex<Option<notify::RecommendedWatcher>>,
}

#[derive(Debug)]
struct ArtifactPath {
	artifact: tg::artifact::Id,
	id: u64,
	mtime: Option<[u8; 16]>,
}

impl FileSystemMonitor {
	/// Start the file system watcher.
	pub fn start(server: &Server) -> tg::Result<Self> {
		let paths = lru::LruCache::unbounded_with_hasher(fnv::FnvBuildHasher::default());
		let file_system_monitor = Self {
			inner: Arc::new(Inner {
				paths: std::sync::RwLock::new(paths),
				watcher: std::sync::Mutex::new(None),
			}),
		};

		let current_runtime_handle = tokio::runtime::Handle::current();
		let watcher = notify::recommended_watcher({
			let file_system_monitor = file_system_monitor.clone();
			let server = server.clone();
			move |event: Result<notify::Event, notify::Error>| {
				let Ok(event) =
					event.inspect_err(|error| tracing::error!(%error, "failed to get event"))
				else {
					return;
				};
				if !event.kind.is_modify() {
					return;
				}
				current_runtime_handle.spawn({
					let file_system_monitor = file_system_monitor.clone();
					let server = server.clone();
					async move {
						let Ok(mut connection) =
							server.inner.database.connection().await.inspect_err(
								|error| tracing::error!(%error, "failed to get database connection"),
							)
						else {
							return;
						};
						let Ok(transaction) = connection.transaction().await.inspect_err(
							|error| tracing::error!(%error, "failed to create database transaction"),
						) else {
							return;
						};
						for path in event.paths {
							let Ok::<tg::Path, _>(path) = path.try_into() else {
								continue;
							};

							// Update the cache.
							file_system_monitor
								.inner
								.paths
								.write()
								.unwrap()
								.put(path.clone(), ());

							// Update the database.
							server
								.remove_artifact_at_path(&path, &transaction)
								.await
								.inspect_err(|error| {
									tracing::error!( %path, %error, "failed to remove artifact path");
								})
								.ok();
						}
						transaction
							.commit()
							.await
							.inspect_err(
								|error| tracing::error!(%error, "failed to commit the transaction"),
							)
							.ok();
					}
				});
			}
		})
		.map_err(|source| tg::error!(!source, "failed to create watcher"))?;

		file_system_monitor
			.inner
			.watcher
			.lock()
			.unwrap()
			.replace(watcher);
		Ok(file_system_monitor)
	}

	/// Stop the file watcher.
	pub fn stop(&self) {
		self.inner.watcher.lock().unwrap().take();
		self.inner.paths.write().unwrap().clear();
	}

	/// Get the current list of paths that are being watched.
	pub fn paths(&self) -> Vec<tg::Path> {
		self.inner
			.paths
			.read()
			.unwrap()
			.iter()
			.map(|(k, ())| k.clone())
			.collect()
	}

	/// Add a path to the watcher.
	pub fn add(&self, path: tg::Path) -> tg::Result<()> {
		let path = path.normalize();
		if self.inner.paths.read().unwrap().contains(&path) {
			return Ok(());
		}

		loop {
			// Get the watcher, bailing out if it's been removed.
			let mut watcher_ = self.inner.watcher.lock().unwrap();
			let Some(watcher) = watcher_.as_mut() else {
				return Ok(());
			};

			// Try and add the watch.
			let Err(source) = watcher.watch(path.as_ref(), notify::RecursiveMode::Recursive) else {
				break;
			};

			// If we've hit the limit of files to watch, pop from the cache and try again.
			if let notify::ErrorKind::MaxFilesWatch = &source.kind {
				if self.inner.paths.write().unwrap().pop_lru().is_some() {
					continue;
				}
			}

			// Something else went wrong.
			return Err(tg::error!(!source, %path, "failed to add path to file watcher"));
		}

		// Add the path and the current time.
		self.inner.paths.write().unwrap().push(path, ());
		Ok(())
	}

	/// Remove a path from the file system watcher.
	pub fn remove(&self, path: &tg::Path) -> tg::Result<()> {
		let path = path.clone().normalize();
		if !self.inner.paths.read().unwrap().contains(&path) {
			return Ok(());
		}
		let mut watcher = self.inner.watcher.lock().unwrap();
		let Some(watcher) = watcher.as_mut() else {
			return Ok(());
		};
		watcher
			.unwatch(path.as_ref())
			.map_err(|source| tg::error!(!source, %path, "failed to remove path from watcher"))?;
		self.inner.paths.write().unwrap().pop(&path);
		Ok(())
	}
}

impl Server {
	pub(crate) fn get_watches(&self) -> Vec<tg::Path> {
		let paths = self
			.inner
			.file_system_monitor
			.lock()
			.unwrap()
			.as_ref()
			.map(crate::fsm::FileSystemMonitor::paths)
			.unwrap_or_default();
		paths
	}

	pub(crate) fn remove_watch(&self, path: &tg::Path) -> tg::Result<()> {
		self.inner
			.file_system_monitor
			.lock()
			.unwrap()
			.as_ref()
			.map_or(Ok(()), |f| f.remove(path))
	}

	pub(crate) async fn add_artifact_at_path(
		&self,
		path: &tg::Path,
		artifact: tg::artifact::Id,
		watch: bool,
		transaction: &Transaction<'_>,
	) -> tg::Result<()> {
		// Add to the file watcher if appropriate.
		if watch {
			self.inner
				.file_system_monitor
				.lock()
				.unwrap()
				.as_ref()
				.map(|f| f.add(path.clone()));
		}

		// Update the database.
		self.add_artifact_path(path, artifact, transaction)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to add the artifact path"))?;

		Ok(())
	}

	pub(crate) async fn remove_artifact_at_path(
		&self,
		path: &tg::Path,
		transaction: &Transaction<'_>,
	) -> tg::Result<()> {
		let Some(artifact_path) = self
			.try_get_artifact_path(path, transaction)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the artifact at path"))?
		else {
			return Ok(());
		};
		self.remove_artifact_path(&artifact_path, transaction)
			.await
			.map_err(|source| tg::error!(!source, "failed to remove the artifact"))?;
		Ok(())
	}

	pub(crate) async fn try_get_artifact_at_path(
		&self,
		path: &tg::Path,
		transaction: &Transaction<'_>,
	) -> tg::Result<Option<tg::artifact::Id>> {
		// Try to get the artifact path.
		let Some(artifact_path) = self
			.try_get_artifact_path(path, transaction)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to get artifact path"))?
		else {
			return Ok(None);
		};

		// If the mtimes don't match, remove the artifact path.
		if !matches!(artifact_path.artifact, tg::artifact::Id::Directory(_)) {
			let mtime = get_mtime(path).await?;
			if Some(mtime) != artifact_path.mtime {
				self.remove_artifact_path(&artifact_path, transaction)
					.await
					.map_err(|source| {
						tg::error!(!source, %path, "failed to remove 
					artifact path")
					})?;
				return Ok(None);
			}
		}
		Ok(Some(artifact_path.artifact))
	}

	async fn add_artifact_path(
		&self,
		path: &tg::Path,
		artifact: tg::artifact::Id,
		transaction: &Transaction<'_>,
	) -> tg::Result<()> {
		let path = path.clone().normalize();
		let mut components = path
			.clone()
			.normalize()
			.into_components()
			.into_iter()
			.skip(1)
			.map(tg::path::Component::unwrap_normal)
			.peekable();

		#[derive(serde::Deserialize, Debug)]
		struct Row {
			id: u64,
		}

		let mtime = if matches!(artifact, tg::artifact::Id::Directory(_)) {
			None
		} else {
			Some(get_mtime(&path).await?)
		};

		let mut parent = 0;
		while let Some(name) = components.next() {
			// If this is the last component of the path, get the artifact and mtime.
			let (mtime, artifact) = if components.peek().is_none() {
				(mtime, Some(artifact.clone()))
			} else {
				(None, None)
			};
			// If parent is not null we can upsert the row.
			let p = self.inner.database.p();
			let statement = formatdoc!(
				"
				insert into artifact_paths (parent, name, mtime, artifact)
				values ({p}1, {p}2, {p}3, {p}4)
				on conflict (parent, name)
				do update set
					mtime = {p}3,
					artifact = {p}4
				returning id;
				"
			);
			let params = db::params![parent, name, mtime, artifact];
			let row = transaction
				.query_one_into::<Row>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
			parent = row.id;
		}
		Ok(())
	}

	async fn remove_artifact_path(
		&self,
		artifact_path: &ArtifactPath,
		transaction: &Transaction<'_>,
	) -> tg::Result<()> {
		#[derive(serde::Deserialize)]
		struct Row {
			parent: u64,
		}

		// Invalidate the parents.
		let mut id = artifact_path.id;
		while id != 0 {
			let p = self.inner.database.p();
			let statement = formatdoc!(
				"
				update artifact_paths
				set 
					artifact = null,
					mtime = null
				where id = {p}1
				returning parent;
				"
			);
			let params = db::params![id];
			let row = transaction
				.query_one_into::<Row>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			id = row.parent;
		}
		Ok(())
	}

	async fn try_get_artifact_path(
		&self,
		path: &tg::Path,
		transaction: &Transaction<'_>,
	) -> tg::Result<Option<ArtifactPath>> {
		let path = path.clone().normalize();
		let mut components = path
			.clone()
			.normalize()
			.into_components()
			.into_iter()
			.skip(1)
			.map(tg::path::Component::unwrap_normal)
			.peekable();

		#[derive(serde::Deserialize)]
		struct Row {
			id: u64,
			mtime: Option<[u8; 16]>,
			artifact: Option<tg::artifact::Id>,
		}
		let mut parent = 0;
		while let Some(name) = components.next() {
			let p = self.inner.database.p();
			let statement = formatdoc!(
				"
				select id, mtime, artifact
				from artifact_paths
				where name = {p}1 and parent = {p}2
				"
			);
			let params = db::params![name, parent];
			let row = transaction
				.query_optional_into::<Row>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
			let Some(row) = row else {
				return Ok(None);
			};

			// If this is not the last component, continue.
			if components.peek().is_some() {
				parent = row.id;
				continue;
			}

			// Otherwise update the artifact/mtime fields and continue.
			let Some(artifact) = row.artifact else {
				return Ok(None);
			};
			let mtime = row.mtime;
			return Ok(Some(ArtifactPath {
				id: row.id,
				mtime,
				artifact,
			}));
		}
		Ok(None)
	}
}

async fn get_mtime(path: &tg::Path) -> tg::Result<[u8; 16]> {
	let metadata = tokio::fs::symlink_metadata(path)
		.await
		.map_err(|source| tg::error!(!source, %path, "failed to get file metadata"))?;

	let mtime = metadata
		.modified()
		.map_err(|source| tg::error!(!source, "failed to get mtime"))?
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_micros()
		.to_le_bytes();

	Ok(mtime)
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_get_watches_request(
		&self,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the paths.
		let paths = self.inner.tg.list_watches().await?;

		// Create the body.
		let body = serde_json::to_vec(&paths)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();
		Ok(response)
	}

	pub async fn handle_remove_watch_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["packages", path] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let path = serde_urlencoded::from_str(path)
			.map_err(|source| tg::error!(!source, "failed to serialize path"))?;

		// Remove the path.
		self.inner
			.tg
			.remove_watch(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to remove the path"))?;

		// Create the response
		let response = http::Response::builder().body(empty()).unwrap();
		Ok(response)
	}
}
