use {
	futures::{FutureExt as _, future::BoxFuture},
	provider::Provider,
	std::{
		os::fd::OwnedFd,
		path::Path,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_server_core::vfs::{Kind, MountArg},
	tangram_vfs as vfs,
};

#[cfg(target_os = "macos")]
mod fskit;

pub mod provider;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub store: Mutex<Option<Box<dyn tangram_server_vfs_core::Server>>>,
}

pub struct Server {
	inner: Inner,
	#[cfg(target_os = "linux")]
	provider: provider::Weak,
}

enum Inner {
	#[cfg(target_os = "macos")]
	Fskit(fskit::Server),

	#[cfg(target_os = "linux")]
	Fuse(vfs::fuse::Server<Provider>),

	Nfs(vfs::nfs::Server<Provider>),

	#[cfg(target_os = "linux")]
	Virtiofs(vfs::virtiofs::Server),
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		let store = Mutex::new(None);
		let state = State { store };

		Self(Arc::new(state))
	}

	pub async fn mount_store(
		&self,
		server: &tangram_server_core::Server,
		arg: MountArg<'_>,
	) -> tg::Result<()> {
		let MountArg {
			kind,
			options,
			origin,
			path,
			principal,
			recvfd,
		} = arg;
		let store = Server::start(server, kind, path, options, origin, principal, recvfd).await?;
		let store: Box<dyn tangram_server_vfs_core::Server> = Box::new(store);
		self.store.lock().unwrap().replace(store);

		Ok(())
	}

	#[must_use]
	pub fn mounted(&self) -> bool {
		self.store.lock().unwrap().is_some()
	}
}

impl Server {
	pub async fn start(
		server: &tangram_server_core::Server,
		kind: Kind,
		path: &Path,
		options: tangram_server_core::config::Vfs,
		origin: tangram_server_core::context::Origin,
		principal: Option<tg::Principal>,
		recvfd: Option<OwnedFd>,
	) -> tg::Result<Self> {
		// Remove a file at the path if one exists.
		tokio::fs::remove_file(path).await.ok();

		// Create a directory at the path if necessary.
		tokio::fs::create_dir_all(path).await.ok();

		// Create the provider.
		let provider = Provider::new(server, origin, principal.clone())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the vfs provider"))?;

		#[cfg(target_os = "linux")]
		let weak = provider.downgrade();
		let inner = match kind {
			Kind::Fskit => {
				#[cfg(target_os = "macos")]
				{
					let principal = principal.unwrap_or(tg::Principal::Anonymous);
					let fskit = fskit::Server::start(server, path, principal).await?;
					Inner::Fskit(fskit)
				}
				#[cfg(not(target_os = "macos"))]
				{
					let _ = server;
					return Err(tg::error!("fskit is only supported on macos"));
				}
			},
			Kind::Fuse => {
				#[cfg(target_os = "linux")]
				{
					let fuse = &options.fuse;
					let io = match options.io {
						tangram_server_core::config::VfsIo::Auto => vfs::fuse::Io::Auto,
						tangram_server_core::config::VfsIo::IoUring => vfs::fuse::Io::IoUring,
						tangram_server_core::config::VfsIo::ReadWrite => vfs::fuse::Io::ReadWrite,
					};
					let passthrough = match options.passthrough {
						tangram_server_core::config::VfsPassthrough::Auto => {
							vfs::fuse::Passthrough::Auto
						},
						tangram_server_core::config::VfsPassthrough::Disabled => {
							vfs::fuse::Passthrough::Disabled
						},
						tangram_server_core::config::VfsPassthrough::Required => {
							vfs::fuse::Passthrough::Required
						},
					};
					let options = vfs::fuse::Options {
						io,
						io_uring_completion_batch_size: fuse.io_uring_completion_batch_size,
						io_uring_max_retired_slots_per_worker: fuse
							.io_uring_max_retired_slots_per_worker,
						io_uring_max_slots_per_queue: fuse.io_uring_max_slots_per_queue,
						io_uring_max_worker_count: fuse.io_uring_max_worker_count,
						io_uring_payload_memory_budget: fuse.io_uring_payload_memory_budget,
						io_uring_startup_timeout: fuse.io_uring_startup_timeout,
						max_write: fuse.max_write,
						mount_ready_timeout: fuse.mount_ready_timeout,
						passthrough,
						read_write_async_concurrency: fuse.read_write_async_concurrency,
						read_write_async_queue_depth: fuse.read_write_async_queue_depth,
						read_write_max_reader_count: fuse.read_write_max_reader_count,
						sqpoll: options.sqpoll,
						sqpoll_entries: fuse.sqpoll_entries,
						sqpoll_idle: fuse.sqpoll_idle,
					};
					// Without a sandbox socket, mount with the fusermount3 helper after unmounting a stale mount.
					let recvfd = match recvfd {
						None => {
							vfs::fuse::Server::<Provider>::unmount(path).await.ok();
							vfs::fuse::fusermount3(path).map_err(|error| {
								tg::error!(!error, "failed to start the FUSE mount helper")
							})?
						},
						Some(recvfd) => recvfd,
					};
					let fuse = vfs::fuse::Server::start(provider, path, options, recvfd)
						.await
						.map_err(|error| tg::error!(!error, "failed to start the FUSE server"))?;
					Inner::Fuse(fuse)
				}
				#[cfg(not(target_os = "linux"))]
				{
					let _ = (options, recvfd);
					return Err(tg::error!("the FUSE VFS is only supported on Linux"));
				}
			},
			Kind::Nfs => {
				let nfs = &options.nfs;
				let options = vfs::nfs::Options {
					lease_reaper_interval: nfs.lease_reaper_interval,
					lease_time: nfs.lease_time,
					max_array_length: nfs.max_array_length,
					max_compound_operations: nfs.max_compound_operations,
					max_connections: nfs.max_connections,
					max_global_in_flight_requests: nfs.max_global_in_flight_requests,
					max_in_flight_requests_per_connection: nfs
						.max_in_flight_requests_per_connection,
					max_name_length: nfs.max_name_length,
					max_pending_responses: nfs.max_pending_responses,
					max_read_size: nfs.max_read_size,
					max_record_size: nfs.max_record_size,
					port: nfs.port,
					readdir_concurrency: nfs.readdir_concurrency,
				};
				let nfs = vfs::nfs::Server::start(provider, path, &nfs.host, options)
					.await
					.map_err(|error| tg::error!(!error, "failed to start the NFS server"))?;
				Inner::Nfs(nfs)
			},
		};

		let server = Self {
			inner,
			#[cfg(target_os = "linux")]
			provider: weak,
		};
		Ok(server)
	}

	#[cfg(target_os = "linux")]
	pub async fn start_virtiofs(
		server: &tangram_server_core::Server,
		socket: &Path,
		dax: Option<u64>,
		origin: tangram_server_core::context::Origin,
		principal: Option<tg::Principal>,
	) -> tg::Result<Self> {
		let provider = Provider::new(server, origin, principal)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the vfs provider"))?;
		let weak = provider.downgrade();
		let dax_window_size = dax.unwrap_or(0);
		let server = vfs::virtiofs::Server::start(provider, socket, dax_window_size)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the virtiofsd server"))?;
		let server = Self {
			inner: Inner::Virtiofs(server),
			provider: weak,
		};
		Ok(server)
	}

	pub async fn unmount(kind: Kind, path: &Path) -> tg::Result<()> {
		match kind {
			Kind::Fskit => {
				#[cfg(target_os = "macos")]
				{
					fskit::Server::unmount(path).await?;
				}
				#[cfg(not(target_os = "macos"))]
				{
					let _ = path;
					return Err(tg::error!("fskit is only supported on macos"));
				}
			},
			Kind::Fuse => {
				#[cfg(target_os = "linux")]
				{
					vfs::fuse::Server::<Provider>::unmount(path)
						.await
						.map_err(|error| tg::error!(!error, "failed to unmount"))?;
				}
				#[cfg(not(target_os = "linux"))]
				{
					let _ = path;
					return Err(tg::error!("fuse is only supported on linux"));
				}
			},
			Kind::Nfs => vfs::nfs::unmount(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to unmount"))?,
		}
		Ok(())
	}

	#[cfg(target_os = "linux")]
	#[must_use]
	pub fn provider(&self) -> &provider::Weak {
		&self.provider
	}

	pub fn stop(&self) {
		match &self.inner {
			#[cfg(target_os = "macos")]
			Inner::Fskit(_) => {},
			#[cfg(target_os = "linux")]
			Inner::Fuse(server) => server.stop(),
			Inner::Nfs(server) => server.stop(),
			#[cfg(target_os = "linux")]
			Inner::Virtiofs(server) => server.stop(),
		}
	}

	pub async fn wait(self) {
		match self.inner {
			#[cfg(target_os = "macos")]
			Inner::Fskit(server) => {
				if let Err(error) = fskit::Server::unmount(server.path()).await {
					tracing::error!(?error, "failed to unmount the fskit vfs");
				}
			},
			#[cfg(target_os = "linux")]
			Inner::Fuse(server) => {
				server.wait().await;
			},
			Inner::Nfs(server) => {
				server.wait().await;
			},
			#[cfg(target_os = "linux")]
			Inner::Virtiofs(server) => {
				server.wait().await;
			},
		}
	}
}

impl Default for Component {
	fn default() -> Self {
		Self::new()
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn stop<'a>(&'a self, _server: &'a tangram_server_core::Server) -> BoxFuture<'a, ()> {
		async move {
			// Stop the store VFS.
			let store = self.store.lock().unwrap().take();
			if let Some(store) = store {
				store.stop();
				store.wait().await;
				tracing::trace!("vfs task");
			}
		}
		.boxed()
	}

	fn abort(&self) {
		self.store.lock().unwrap().take();
	}
}

impl tangram_server_core::vfs::Component for Component {
	fn mount<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		arg: MountArg<'a>,
	) -> BoxFuture<'a, tg::Result<Box<dyn tangram_server_vfs_core::Server>>> {
		async move {
			let MountArg {
				kind,
				options,
				origin,
				path,
				principal,
				recvfd,
			} = arg;
			let server =
				Server::start(server, kind, path, options, origin, principal, recvfd).await?;
			let server: Box<dyn tangram_server_vfs_core::Server> = Box::new(server);
			Ok(server)
		}
		.boxed()
	}

	fn mount_store<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		arg: MountArg<'a>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.mount_store(server, arg).boxed()
	}

	fn mounted(&self) -> bool {
		self.mounted()
	}

	#[cfg(target_os = "linux")]
	fn start_virtiofs<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		socket: &'a Path,
		dax: Option<u64>,
		origin: tangram_server_core::context::Origin,
		principal: Option<tg::Principal>,
	) -> BoxFuture<'a, tg::Result<Box<dyn tangram_server_vfs_core::Server>>> {
		async move {
			let server = Server::start_virtiofs(server, socket, dax, origin, principal).await?;
			let server: Box<dyn tangram_server_vfs_core::Server> = Box::new(server);
			Ok(server)
		}
		.boxed()
	}

	fn unmount<'a>(&'a self, kind: Kind, path: &'a Path) -> BoxFuture<'a, tg::Result<()>> {
		Server::unmount(kind, path).boxed()
	}
}

impl tangram_server_vfs_core::Server for Server {
	#[cfg(target_os = "linux")]
	fn provider(&self) -> std::sync::Arc<dyn tangram_server_vfs_core::Provider> {
		std::sync::Arc::new(self.provider().clone())
	}

	fn stop(&self) {
		self.stop();
	}

	fn wait(self: Box<Self>) -> BoxFuture<'static, ()> {
		(*self).wait().boxed()
	}
}
