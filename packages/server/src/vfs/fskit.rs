use {
	num::ToPrimitive as _,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
	tokio::{
		io::{AsyncBufReadExt as _, AsyncWriteExt as _},
		net::UnixStream,
	},
};

#[derive(Clone)]
pub struct Server {
	path: PathBuf,
}

#[derive(serde::Serialize)]
struct Request {
	#[serde(rename = "data_directory")]
	data_directory: PathBuf,

	#[serde(rename = "mount_path")]
	mount_path: PathBuf,

	#[serde(rename = "object_store", skip_serializing_if = "Option::is_none")]
	object_store: Option<ObjectStore>,

	socket: PathBuf,

	token: String,

	#[serde(rename = "type")]
	type_: &'static str,
}

/// The object store the fast path opens. It is sent only if the store is an lmdb store, because the fast path can read no other kind.
#[derive(serde::Serialize)]
struct ObjectStore {
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
	pub async fn start(server: &crate::Server, path: &Path) -> tg::Result<Self> {
		// Ask the app to mount if the app started this server, because only the app can prompt for the file system extension's approval. Otherwise mount directly, which is the path the test harness takes.
		if std::env::var_os("TANGRAM_MACOS_APP_SOCKET").is_some() {
			Self::start_with_app(server, path).await
		} else {
			Self::start_with_mount(server, path).await
		}
	}

	async fn start_with_app(server: &crate::Server, path: &Path) -> tg::Result<Self> {
		// Get the environment the app set.
		let socket = std::env::var_os("TANGRAM_MACOS_APP_SOCKET")
			.ok_or_else(|| tg::error!("missing the macos app socket environment variable"))?;
		let token = std::env::var("TANGRAM_MACOS_APP_TOKEN").map_err(|error| {
			tg::error!(!error, "missing the macos app token environment variable")
		})?;
		let group_socket = Self::group_socket()?;

		// Create the request.
		let object_store = Self::object_store(server);
		let request = Request {
			data_directory: server.path.clone(),
			mount_path: path.to_path_buf(),
			object_store,
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
		})
	}

	async fn start_with_mount(server: &crate::Server, path: &Path) -> tg::Result<Self> {
		let group_socket = Self::group_socket()?;
		let object_store = Self::object_store(server);
		let options = Self::options(object_store.as_ref(), &group_socket);
		let output = tokio::process::Command::new("/sbin/mount")
			.arg("-F")
			.arg("-t")
			.arg("tangram")
			.arg("-o")
			.arg(options)
			.arg(&server.path)
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
		})
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}

	pub async fn unmount(path: &Path) -> tg::Result<()> {
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

	fn group_socket() -> tg::Result<PathBuf> {
		let group_socket = std::env::var_os("TANGRAM_MACOS_APP_GROUP_SOCKET")
			.ok_or_else(|| tg::error!("missing the macos app group socket environment variable"))?;
		Ok(PathBuf::from(group_socket))
	}

	fn object_store(server: &crate::Server) -> Option<ObjectStore> {
		match &server.config.object.store {
			crate::config::ObjectStore::Lmdb(config) => Some(ObjectStore {
				map_size: config.map_size.to_u64().unwrap(),
				path: server.path.join(&config.path),
				posix_sem_prefix: config.resolved_posix_sem_prefix(),
			}),
			_ => None,
		}
	}

	/// Builds the value of the mount's `-o` option. The generic options come first, which the mount command consumes itself, and it forwards the rest to the file system extension. A path containing a comma cannot be represented, so it is omitted and the extension uses its default.
	fn options(object_store: Option<&ObjectStore>, socket: &Path) -> String {
		let mut options = vec![
			"nobrowse".to_owned(),
			"nodev".to_owned(),
			"nosuid".to_owned(),
		];
		let socket = socket.display().to_string();
		if socket.contains(',') {
			tracing::error!(
				"the socket path contains a comma, so it cannot be sent to the file system extension"
			);
		} else {
			options.push(format!("socket={socket}"));
		}
		if let Some(object_store) = object_store {
			options.push(format!("object_store_map_size={}", object_store.map_size));
			let path = object_store.path.display().to_string();
			if path.contains(',') {
				tracing::error!(
					"the object store path contains a comma, so it cannot be sent to the file system extension"
				);
			} else {
				options.push(format!("object_store_path={path}"));
			}
			if let Some(posix_sem_prefix) = &object_store.posix_sem_prefix {
				if posix_sem_prefix.contains(',') {
					tracing::error!(
						"the object store semaphore prefix contains a comma, so it cannot be sent to the file system extension"
					);
				} else {
					options.push(format!("object_store_posix_sem_prefix={posix_sem_prefix}"));
				}
			}
		}
		options.join(",")
	}
}
