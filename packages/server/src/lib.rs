use self::database::Database;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
	stream::{BoxStream, FuturesUnordered},
	TryStreamExt,
};
use itertools::Itertools;
use std::{
	collections::HashMap,
	os::fd::AsRawFd,
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_error::{Result, Wrap, WrapErr};
use tg::util::rmrf;

mod artifact;
mod build;
mod clean;
mod database;
mod migrations;
mod object;
mod package;
mod serve;
mod user;

/// A server.
#[derive(Clone)]
pub struct Server {
	inner: Arc<Inner>,
}

struct Inner {
	build_context:
		std::sync::RwLock<HashMap<tg::build::Id, Arc<BuildContext>, fnv::FnvBuildHasher>>,
	build_semaphore: Arc<tokio::sync::Semaphore>,
	database: Database,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	http_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	http_task_stop_sender: tokio::sync::watch::Sender<bool>,
	local_queue_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	local_queue_task_stop_sender: tokio::sync::watch::Sender<bool>,
	local_queue_task_wake_sender: tokio::sync::watch::Sender<()>,
	lockfile: std::sync::Mutex<Option<tokio::fs::File>>,
	path: PathBuf,
	remote: Option<Box<dyn tg::Handle>>,
	remote_queue_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	remote_queue_task_stop_sender: tokio::sync::watch::Sender<bool>,
	shutdown: tokio::sync::watch::Sender<bool>,
	shutdown_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	version: String,
	vfs: std::sync::Mutex<Option<tangram_vfs::Server>>,
}

struct BuildContext {
	children: Option<tokio::sync::watch::Sender<()>>,
	depth: u64,
	log: Option<tokio::sync::watch::Sender<()>>,
	outcome: Option<tokio::sync::watch::Sender<()>>,
	stop: tokio::sync::watch::Sender<bool>,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub struct Options {
	pub addr: tg::Addr,
	pub path: PathBuf,
	pub remote: Option<RemoteOptions>,
	pub version: String,
}

pub struct RemoteOptions {
	pub build: Option<RemoteBuildOptions>,
	pub tg: Box<dyn tg::Handle>,
}

pub struct RemoteBuildOptions {
	pub enable: bool,
	pub hosts: Option<Vec<tg::System>>,
}

impl Server {
	#[allow(clippy::too_many_lines)]
	pub async fn start(options: Options) -> Result<Server> {
		// Get the addr.
		let addr = options.addr;

		// Get the path.
		let path = options.path;

		// Ensure the path exists.
		tokio::fs::create_dir_all(&path)
			.await
			.wrap_err("Failed to create the directory.")?;

		// Acquire the lockfile.
		let lockfile = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(path.join("lock"))
			.await
			.wrap_err("Failed to open the lockfile.")?;
		let ret = unsafe { libc::flock(lockfile.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
		if ret != 0 {
			return Err(std::io::Error::last_os_error().wrap("Failed to acquire the lockfile."));
		}
		let lockfile = std::sync::Mutex::new(Some(lockfile));

		// Migrate the path.
		Self::migrate(&path).await?;

		// Write the PID file.
		tokio::fs::write(&path.join("server.pid"), std::process::id().to_string())
			.await
			.wrap_err("Failed to write the PID file.")?;

		// Remove an existing socket file.
		rmrf(&path.join("socket"))
			.await
			.wrap_err("Failed to remove an existing socket file.")?;

		// Create the build context.
		let build_context = std::sync::RwLock::new(HashMap::default());

		// Create the build semaphore.
		let build_semaphore = Arc::new(tokio::sync::Semaphore::new(
			std::thread::available_parallelism().unwrap().get(),
		));

		// Open the database.
		let database_path = path.join("database");
		let database = Database::new(&database_path).await?;

		// Create the file system semaphore.
		let file_descriptor_semaphore = tokio::sync::Semaphore::new(16);

		// Create the http task.
		let http_task = std::sync::Mutex::new(None);

		// Create the http task stop channel.
		let (http_task_stop_sender, http_task_stop_receiver) = tokio::sync::watch::channel(false);

		// Create the local queue task.
		let local_queue_task = std::sync::Mutex::new(None);

		// Create the local queue task wake channel.
		let (local_queue_task_wake_sender, local_queue_task_wake_receiver) =
			tokio::sync::watch::channel(());

		// Create the local queue task stop channel.
		let (local_queue_task_stop_sender, local_queue_task_stop_receiver) =
			tokio::sync::watch::channel(false);

		// Get the remote.
		let remote = if let Some(remote) = options.remote {
			Some(remote.tg)
		} else {
			None
		};

		// Create the remote queue task.
		let remote_queue_task = std::sync::Mutex::new(None);

		// Create the remote queue task stop channel.
		let (remote_queue_task_stop_sender, remote_queue_task_stop_receiver) =
			tokio::sync::watch::channel(false);

		// Create the shutdown channel.
		let (shutdown, _) = tokio::sync::watch::channel(false);

		// Create the shutdown task.
		let shutdown_task = std::sync::Mutex::new(None);

		// Get the version.
		let version = options.version;

		// Create the vfs.
		let vfs = std::sync::Mutex::new(None);

		// Create the server.
		let inner = Arc::new(Inner {
			build_context,
			build_semaphore,
			database,
			file_descriptor_semaphore,
			http_task,
			http_task_stop_sender,
			local_queue_task,
			local_queue_task_stop_sender,
			local_queue_task_wake_sender,
			lockfile,
			path,
			remote,
			remote_queue_task,
			remote_queue_task_stop_sender,
			shutdown,
			shutdown_task,
			version,
			vfs,
		});
		let server = Server { inner };

		// Empty the queue.
		{
			let db = server.inner.database.get().await?;
			let statement = "
				delete from queue;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute([])
				.wrap_err("Failed to execute the query.")?;
		}

		// Terminate unfinished builds.
		{
			let db = server.inner.database.get().await?;
			let statement = r#"
				update builds
				set state = json_set(
					state,
					'$.status', 'finished',
					'$.outcome', json('{"kind":"terminated"}')
				)
				where state->>'status' != 'finished';
			"#;
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute([])
				.wrap_err("Failed to execute the query.")?;
		}

		// Start the vfs.
		let vfs = tangram_vfs::Server::start(&server, &server.artifacts_path())
			.await
			.wrap_err("Failed to start the vfs.")?;
		server.inner.vfs.lock().unwrap().replace(vfs);

		// Start the build queue task.
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				let result = server
					.local_queue_task(
						local_queue_task_wake_receiver,
						local_queue_task_stop_receiver,
					)
					.await;
				match result {
					Ok(()) => Ok(()),
					Err(error) => {
						tracing::error!(?error);
						Err(error)
					},
				}
			}
		});
		server.inner.local_queue_task.lock().unwrap().replace(task);

		// Start the build queue remote task.
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				let result = server
					.remote_queue_task(remote_queue_task_stop_receiver)
					.await;
				match result {
					Ok(()) => Ok(()),
					Err(error) => {
						tracing::error!(?error);
						Err(error)
					},
				}
			}
		});
		server.inner.remote_queue_task.lock().unwrap().replace(task);

		// Start the http task.
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				match server.serve(addr, http_task_stop_receiver).await {
					Ok(()) => Ok(()),
					Err(error) => {
						tracing::error!(?error);
						Err(error)
					},
				}
			}
		});
		server.inner.http_task.lock().unwrap().replace(task);

		Ok(server)
	}

	pub fn stop(&self) {
		let server = self.clone();
		let task = tokio::spawn(async move {
			// Stop the http task.
			server.inner.http_task_stop_sender.send_replace(true);

			// Stop the local queue task.
			server.inner.local_queue_task_stop_sender.send_replace(true);

			// Stop the remote queue task.
			server
				.inner
				.remote_queue_task_stop_sender
				.send_replace(true);

			// Join the http task.
			let task = server.inner.http_task.lock().unwrap().take();
			if let Some(task) = task {
				match task.await {
					Ok(result) => Ok(result),
					Err(error) if error.is_cancelled() => Ok(Ok(())),
					Err(error) => Err(error),
				}
				.unwrap()?;
			}

			// Join the local queue task.
			let local_queue_task = server
				.inner
				.local_queue_task
				.lock()
				.unwrap()
				.take()
				.unwrap();
			local_queue_task.await.unwrap()?;

			// Join the remote queue task.
			let remote_queue_task = server
				.inner
				.remote_queue_task
				.lock()
				.unwrap()
				.take()
				.unwrap();
			remote_queue_task.await.unwrap()?;

			// Stop running builds.
			for context in server.inner.build_context.read().unwrap().values() {
				context.stop.send_replace(true);
			}

			// Join running builds.
			let tasks = server
				.inner
				.build_context
				.read()
				.unwrap()
				.values()
				.cloned()
				.filter_map(|context| context.task.lock().unwrap().take())
				.collect_vec();
			tasks
				.into_iter()
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await
				.unwrap();

			// Remove running builds.
			server.inner.build_context.write().unwrap().clear();

			// Join the vfs.
			let vfs = server.inner.vfs.lock().unwrap().clone();
			if let Some(vfs) = vfs {
				vfs.stop();
				vfs.join().await?;
			}

			// Release the lockfile.
			server.inner.lockfile.lock().unwrap().take();

			Ok(())
		});

		self.inner.shutdown_task.lock().unwrap().replace(task);
		self.inner.shutdown.send_replace(true);
	}

	pub async fn join(&self) -> Result<()> {
		self.inner
			.shutdown
			.subscribe()
			.wait_for(|shutdown| *shutdown)
			.await
			.unwrap();
		let task = self.inner.shutdown_task.lock().unwrap().take();
		if let Some(task) = task {
			task.await.unwrap()?;
		}
		Ok(())
	}

	#[allow(clippy::unused_async)]
	async fn health(&self) -> Result<tg::health::Health> {
		Ok(tg::health::Health {
			version: self.inner.version.clone(),
		})
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.inner.path
	}

	#[must_use]
	pub fn artifacts_path(&self) -> PathBuf {
		self.path().join("artifacts")
	}

	#[must_use]
	pub fn database_path(&self) -> PathBuf {
		self.path().join("database")
	}

	#[must_use]
	pub fn tmp_path(&self) -> PathBuf {
		self.path().join("tmp")
	}
}

#[async_trait]
impl tg::Handle for Server {
	fn clone_box(&self) -> Box<dyn tg::Handle> {
		Box::new(self.clone())
	}

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore {
		&self.inner.file_descriptor_semaphore
	}

	async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> Result<tg::artifact::CheckInOutput> {
		self.check_in_artifact(arg).await
	}

	async fn check_out_artifact(&self, arg: tg::artifact::CheckOutArg) -> Result<()> {
		self.check_out_artifact(arg).await
	}

	async fn try_list_builds(&self, args: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		self.try_list_builds(args).await
	}

	async fn get_build_exists(&self, id: &tg::build::Id) -> Result<bool> {
		self.get_build_exists(id).await
	}

	async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::GetOutput>> {
		self.try_get_build(id).await
	}

	async fn try_put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		state: &tg::build::State,
	) -> Result<tg::build::PutOutput> {
		self.try_put_build(user, id, state).await
	}

	async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		self.get_or_create_build(user, arg).await
	}

	async fn try_dequeue_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::DequeueArg,
	) -> Result<Option<tg::build::DequeueOutput>> {
		self.try_dequeue_build(user, arg).await
	}

	async fn try_get_build_status(&self, id: &tg::build::Id) -> Result<Option<tg::build::Status>> {
		self.try_get_build_status(id).await
	}

	async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()> {
		self.set_build_status(user, id, status).await
	}

	async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Id>>>> {
		self.try_get_build_children(id, None).await
	}

	async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<()> {
		self.add_build_child(user, build_id, child_id).await
	}

	async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetLogArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::LogEntry>>>> {
		self.try_get_build_log(id, arg).await
	}

	async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()> {
		self.add_build_log(user, build_id, bytes).await
	}

	async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Outcome>> {
		self.try_get_build_outcome(id, None).await
	}

	async fn set_build_outcome(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		self.set_build_outcome(user, id, outcome).await
	}

	async fn get_object_exists(&self, id: &tg::object::Id) -> Result<bool> {
		self.get_object_exists(id).await
	}

	async fn try_get_object(&self, id: &tg::object::Id) -> Result<Option<tg::object::GetOutput>> {
		self.try_get_object(id).await
	}

	async fn try_put_object(
		&self,
		id: &tg::object::Id,
		bytes: &Bytes,
	) -> Result<tg::object::PutOutput> {
		self.try_put_object(id, bytes).await
	}

	async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		self.push_object(id).await
	}

	async fn pull_object(&self, id: &tg::object::Id) -> Result<()> {
		self.pull_object(id).await
	}

	async fn search_packages(&self, arg: tg::package::SearchArg) -> Result<Vec<String>> {
		self.search_packages(arg).await
	}

	async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		self.try_get_package(dependency, arg).await
	}

	async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		self.try_get_package_versions(dependency).await
	}

	async fn try_get_package_metadata(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<tg::package::Metadata>> {
		self.try_get_package_metadata(dependency).await
	}

	async fn try_get_package_dependencies(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<tg::Dependency>>> {
		self.try_get_package_dependencies(dependency).await
	}

	async fn publish_package(&self, user: Option<&tg::User>, id: &tg::directory::Id) -> Result<()> {
		self.publish_package(user, id).await
	}

	async fn health(&self) -> Result<tg::health::Health> {
		self.health().await
	}

	async fn clean(&self) -> Result<()> {
		self.clean().await
	}

	async fn stop(&self) -> Result<()> {
		self.stop();
		Ok(())
	}

	async fn create_login(&self) -> Result<tg::user::Login> {
		self.create_login().await
	}

	async fn get_login(&self, id: &tg::Id) -> Result<Option<tg::user::Login>> {
		self.get_login(id).await
	}

	async fn get_user_for_token(&self, token: &str) -> Result<Option<tg::user::User>> {
		self.get_user_for_token(token).await
	}
}
