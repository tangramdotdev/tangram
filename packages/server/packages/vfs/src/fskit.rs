use {
	num::ToPrimitive as _,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_server_core::context::Origin,
	tokio::{
		io::{AsyncBufReadExt as _, AsyncWriteExt as _},
		net::UnixStream,
	},
};

#[cfg(test)]
mod tests;

pub struct Server {
	task: Option<Task<()>>,
}

struct Mount {
	path: PathBuf,
	proxy: Option<Task<()>>,
}

#[derive(serde::Serialize)]
struct Request {
	#[serde(rename = "cache", skip_serializing_if = "Option::is_none")]
	cache: Option<Cache>,

	#[serde(rename = "data_directory")]
	data_directory: PathBuf,

	#[serde(rename = "mount_path")]
	mount_path: PathBuf,

	principal: String,

	socket: PathBuf,

	token: String,

	#[serde(rename = "type")]
	type_: &'static str,
}

/// The local cache the fast path opens.
#[derive(serde::Serialize)]
struct Cache {
	kind: &'static str,

	#[serde(rename = "map_size")]
	map_size: u64,

	path: PathBuf,

	/// The prefix the server opened the lock semaphores with, forwarded to the file system extension so the sandboxed reader shares the writer's lock.
	#[serde(rename = "posix_sem_prefix", skip_serializing_if = "Option::is_none")]
	posix_sem_prefix: Option<String>,
}

#[derive(serde::Deserialize)]
struct Response {
	message: Option<String>,

	#[serde(rename = "type")]
	type_: String,
}

impl Server {
	pub async fn start(
		server: &tangram_server_core::Server,
		path: &Path,
		principal: tg::Principal,
		origin: Origin,
		provider: crate::Provider,
	) -> tg::Result<Self> {
		let server = server.clone();
		let path = path.to_owned();
		let start = async move {
			let result = Mount::start(&server, &path, principal, origin, provider).await;
			let mount = match result {
				Err(error) => {
					Mount::unmount(&path).await.ok();
					return Err(error);
				},
				Ok(mount) => mount,
			};

			Ok(mount.wait())
		};

		Self::start_inner(start).await
	}

	async fn start_inner<F>(
		start: impl Future<Output = tg::Result<F>> + Send + 'static,
	) -> tg::Result<Self>
	where
		F: Future<Output = ()> + Send + 'static,
	{
		// Keep startup and cleanup alive when the caller is cancelled.
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let mut task = Task::spawn(move |stopper| async move {
			let cleanup = match start.await {
				Err(error) => {
					sender.send(Err(error)).ok();
					return;
				},
				Ok(cleanup) => cleanup,
			};
			if sender.send(Ok(())).is_ok() {
				stopper.wait().await;
			}
			cleanup.await;
		});
		task.detach();
		let server = Self { task: Some(task) };
		receiver
			.await
			.map_err(|error| tg::error!(!error, "the fskit mount task exited during startup"))??;

		Ok(server)
	}

	pub fn stop(&self) {
		if let Some(task) = &self.task {
			task.stop();
		}
	}

	pub async fn wait(mut self) {
		let task = self.task.take().unwrap();
		task.stop();
		if let Err(error) = task.wait().await {
			tracing::error!(?error, "the fskit mount task panicked");
		}
	}

	pub async fn unmount(path: &Path) -> tg::Result<()> {
		Mount::unmount(path).await?;
		Ok(())
	}
}

impl Mount {
	async fn start(
		server: &tangram_server_core::Server,
		path: &Path,
		principal: tg::Principal,
		origin: Origin,
		provider: crate::Provider,
	) -> tg::Result<Self> {
		Self::unmount(path).await.ok();
		if matches!(origin, Origin::Sandbox(_)) {
			let (proxy, socket) = Self::start_proxy(provider).await?;
			let mut server = Self::start_with_mount(server, path, principal, Some(&socket)).await?;
			server.proxy = Some(proxy);

			return Ok(server);
		}

		// Ask the app to mount if the app started this server, because only the app can prompt for the file system extension's approval. Otherwise mount directly, which is the path the test harness takes.
		if std::env::var_os("TANGRAM_MACOS_APP_SOCKET").is_some() {
			Self::start_with_app(server, path, principal).await
		} else {
			Self::start_with_mount(server, path, principal, None).await
		}
	}

	async fn start_with_app(
		server: &tangram_server_core::Server,
		path: &Path,
		principal: tg::Principal,
	) -> tg::Result<Self> {
		// Get the environment the app set.
		let socket = std::env::var_os("TANGRAM_MACOS_APP_SOCKET")
			.ok_or_else(|| tg::error!("missing the macos app socket environment variable"))?;
		let token = std::env::var("TANGRAM_MACOS_APP_TOKEN").map_err(|error| {
			tg::error!(!error, "missing the macos app token environment variable")
		})?;
		let group_socket = Self::group_socket(server)?;

		// Create the request.
		let cache = Self::cache(server);
		let request = Request {
			cache,
			data_directory: server.path.clone(),
			mount_path: path.to_path_buf(),
			principal: principal.to_string(),
			socket: group_socket,
			token,
			type_: "mount_vfs",
		};
		let mut request = serde_json::to_vec(&request)
			.map_err(|error| tg::error!(!error, "failed to encode the fskit request"))?;
		request.push(b'\n');

		// Send the request.
		let stream = UnixStream::connect(socket)
			.await
			.map_err(|error| tg::error!(!error, "failed to connect to the macos app"))?;
		let (reader, mut writer) = stream.into_split();
		writer
			.write_all(&request)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the fskit request"))?;
		writer
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush the fskit request"))?;

		// Read the response.
		let mut reader = tokio::io::BufReader::new(reader);
		let mut response = String::new();
		reader
			.read_line(&mut response)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the fskit response"))?;
		let response = serde_json::from_str::<Response>(&response)
			.map_err(|error| tg::error!(!error, "failed to decode the fskit response"))?;
		if response.type_ != "ok" {
			let message = response
				.message
				.unwrap_or_else(|| "unknown error".to_owned());
			return Err(tg::error!("failed to start the fskit vfs: {message}"));
		}

		Ok(Self {
			path: path.to_path_buf(),
			proxy: None,
		})
	}

	async fn start_with_mount(
		server: &tangram_server_core::Server,
		path: &Path,
		principal: tg::Principal,
		proxy: Option<&Path>,
	) -> tg::Result<Self> {
		let group_socket = match proxy {
			Some(path) => path.to_path_buf(),
			None => Self::group_socket(server)?,
		};
		let cache = Self::cache(server);
		let mut options = Self::options(cache.as_ref(), &group_socket, &principal);
		if proxy.is_some() {
			options.push_str(",proxy=1");
		}
		let resource = if proxy.is_some() {
			path.parent()
				.ok_or_else(|| tg::error!("the VFS mount has no parent"))?
		} else {
			&server.path
		};
		let output = tokio::process::Command::new("/sbin/mount")
			.arg("-F")
			.arg("-t")
			.arg("tangram")
			.arg("-o")
			.arg(options)
			.arg(resource)
			.arg(path)
			.output()
			.await
			.map_err(|error| tg::error!(!error, "failed to run the mount command"))?;
		if !output.status.success() {
			let stderr = String::from_utf8_lossy(&output.stderr);
			let stderr = stderr.trim();
			return Err(tg::error!("failed to mount the fskit vfs: {stderr}"));
		}
		Ok(Self {
			path: path.to_path_buf(),
			proxy: None,
		})
	}

	async fn start_proxy(provider: crate::Provider) -> tg::Result<(Task<()>, PathBuf)> {
		// Create the socket directory.
		let home =
			std::env::var_os("HOME").ok_or_else(|| tg::error!("missing the home directory"))?;
		let directory = PathBuf::from(home).join(".tangram/vfs");
		tokio::fs::create_dir_all(&directory)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the VFS socket directory"))?;

		// Bind the listener.
		let socket = directory.join(format!("{}.sock", uuid::Uuid::now_v7().simple()));
		let (listener, guard) = tangram_util::io::unix::bind_listener(&socket)
			.map_err(|error| tg::error!(!error, "failed to bind the VFS socket"))?;

		// Serve the sandbox's VFS requests.
		let task = Task::spawn(move |stopper| async move {
			let _guard = guard;
			let mut tasks = tokio::task::JoinSet::new();
			loop {
				tokio::select! {
					() = stopper.wait() => break,
					Some(result) = tasks.join_next(), if !tasks.is_empty() => {
						if let Err(error) = result {
							tracing::error!(%error, "the VFS request task failed");
						}
					},
					result = listener.accept() => {
						let (stream, _) = match result {
							Ok(output) => output,
							Err(error) => {
								tracing::error!(%error, "failed to accept the VFS connection");
								break;
							},
						};
						let provider = provider.clone();
						tasks.spawn(async move {
							if let Err(error) = tangram_vfs::proxy::serve(stream, &provider).await {
								tracing::debug!(%error, "failed to serve the VFS request");
							}
						});
					},
				}
			}
		});

		Ok((task, socket))
	}

	async fn wait(self) {
		if let Err(error) = Self::unmount(&self.path).await {
			tracing::error!(?error, "failed to unmount the fskit vfs");
		}
		if let Some(proxy) = self.proxy {
			proxy.stop();
			proxy.wait().await.ok();
		}
	}

	async fn unmount(path: &Path) -> tg::Result<()> {
		let output = tokio::process::Command::new("/sbin/umount")
			.arg(path)
			.output()
			.await
			.map_err(|error| tg::error!(!error, "failed to run the umount command"))?;
		if !output.status.success() {
			let stderr = String::from_utf8_lossy(&output.stderr);
			let stderr = stderr.trim();
			return Err(tg::error!("failed to unmount the fskit vfs: {stderr}"));
		}
		Ok(())
	}

	fn group_socket(server: &tangram_server_core::Server) -> tg::Result<PathBuf> {
		let vfs = server.config.vfs.clone().unwrap_or_default();
		let group_socket = vfs.resolved_app_group_socket().ok_or_else(|| {
			tg::error!(
				"missing the macos app group socket environment variable and the vfs app group identifier is not configured"
			)
		})?;
		Ok(group_socket)
	}

	fn cache(server: &tangram_server_core::Server) -> Option<Cache> {
		match &server.config.cache {
			tangram_server_core::config::Cache::Fjall(config) => Some(Cache {
				kind: "fjall",
				map_size: 0,
				path: server.path.join(&config.path),
				posix_sem_prefix: None,
			}),
			tangram_server_core::config::Cache::Lmdb(config) => Some(Cache {
				kind: "lmdb",
				map_size: config.map_size.to_u64().unwrap(),
				path: server.path.join(&config.path),
				posix_sem_prefix: config.resolved_posix_sem_prefix(),
			}),
			tangram_server_core::config::Cache::Rocksdb(config) => Some(Cache {
				kind: "rocksdb",
				map_size: 0,
				path: server.path.join(&config.path),
				posix_sem_prefix: None,
			}),
			_ => None,
		}
	}

	/// Builds the value of the mount's `-o` option. The generic options come first, which the mount command consumes itself, and it forwards the rest to the file system extension. A path containing a comma cannot be represented, so it is omitted and the extension uses its default.
	fn options(cache: Option<&Cache>, socket: &Path, principal: &tg::Principal) -> String {
		let mut options = vec![
			"nobrowse".to_owned(),
			"nodev".to_owned(),
			"nosuid".to_owned(),
		];

		// Add the mount's principal without encoding; its display form contains no commas.
		options.push(format!("principal={principal}"));
		let socket = socket.display().to_string();
		if socket.contains(',') {
			tracing::error!(
				"the socket path contains a comma, so it cannot be sent to the file system extension"
			);
		} else {
			options.push(format!("socket={socket}"));
		}
		if let Some(cache) = cache {
			options.push(format!("cache_kind={}", cache.kind));
			options.push(format!("cache_map_size={}", cache.map_size));
			let path = cache.path.display().to_string();
			if path.contains(',') {
				tracing::error!(
					"the cache path contains a comma, so it cannot be sent to the file system extension"
				);
			} else {
				options.push(format!("cache_path={path}"));
			}
			if let Some(posix_sem_prefix) = &cache.posix_sem_prefix {
				if posix_sem_prefix.contains(',') {
					tracing::error!(
						"the cache semaphore prefix contains a comma, so it cannot be sent to the file system extension"
					);
				} else {
					options.push(format!("cache_posix_sem_prefix={posix_sem_prefix}"));
				}
			}
		}
		options.join(",")
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		self.stop();
	}
}
