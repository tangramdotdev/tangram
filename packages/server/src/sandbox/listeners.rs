use {std::path::PathBuf, tangram_client::prelude::*, tangram_uri::Uri};

#[derive(Clone)]
pub struct Listeners {
	#[cfg(target_os = "linux")]
	pub container: Option<UnixListener>,

	#[cfg(target_os = "macos")]
	pub seatbelt: Option<UnixListener>,

	#[cfg(all(target_os = "linux", feature = "vsock"))]
	pub vm: Option<VsockListener>,
}

#[derive(Clone)]
pub struct UnixListener {
	pub host_url: Uri,
	pub guest_url: Uri,
	pub socket_path: PathBuf,
}

#[cfg(all(target_os = "linux", feature = "vsock"))]
#[derive(Clone)]
pub struct VsockListener {
	pub host_url: Uri,
	pub guest_url: Uri,
}

impl Listeners {
	pub async fn new(
		server_path: &std::path::Path,
		isolation: &crate::config::SandboxIsolation,
	) -> tg::Result<Self> {
		Ok(Self {
			#[cfg(target_os = "linux")]
			container: if isolation.container.is_some() {
				Some(
					UnixListener::new(
						server_path.join("container.socket"),
						tangram_sandbox::Sandbox::guest_tangram_socket_path_from_root(
							std::path::Path::new(""),
						),
					)
					.await?,
				)
			} else {
				None
			},
			#[cfg(target_os = "macos")]
			seatbelt: if isolation.seatbelt.is_some() {
				let socket_path = server_path.join("seatbelt.socket");
				Some(UnixListener::new(socket_path.clone(), socket_path).await?)
			} else {
				None
			},
			#[cfg(all(target_os = "linux", feature = "vsock"))]
			vm: isolation
				.vm
				.as_ref()
				.map(|vm| VsockListener::new(vm.listener_port))
				.transpose()?,
		})
	}
}

impl UnixListener {
	async fn new(socket_path: PathBuf, tangram_socket_path: PathBuf) -> tg::Result<Self> {
		tokio::fs::remove_file(&socket_path).await.ok();
		let host_url = unix_url(&socket_path)?;
		let guest_url = unix_url(&tangram_socket_path)?;
		Ok(Self {
			host_url,
			guest_url,
			socket_path,
		})
	}
}

#[cfg(all(target_os = "linux", feature = "vsock"))]
impl VsockListener {
	fn new(port: Option<u16>) -> tg::Result<Self> {
		let port = if let Some(port) = port.filter(|port| *port != 0) {
			port
		} else {
			let addr = tokio_vsock::VsockAddr::new(tangram_sandbox::vm::VMADDR_CID_ANY, 0);
			let listener = tokio_vsock::VsockListener::bind(addr)
				.map_err(|error| tg::error!(!error, "failed to allocate a vsock port"))?;
			listener
				.local_addr()
				.map_err(|error| tg::error!(!error, "failed to get the allocated vsock port"))?
				.port()
				.try_into()
				.map_err(|_| tg::error!("failed to allocate a valid vsock port"))?
		};
		let host_url = format!(
			"http+vsock://{}:{port}",
			tangram_sandbox::vm::VMADDR_CID_ANY
		)
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the URL"))?;
		let guest_url = format!(
			"http+vsock://{}:{port}",
			tangram_sandbox::vm::VMADDR_CID_HOST
		)
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the URL"))?;
		Ok(Self {
			host_url,
			guest_url,
		})
	}
}

pub fn unix_url(path: &std::path::Path) -> tg::Result<Uri> {
	let path = path.to_str().ok_or_else(|| tg::error!("invalid path"))?;
	Uri::builder()
		.scheme("http+unix")
		.authority(path)
		.path("")
		.build()
		.map_err(|error| tg::error!(!error, "failed to build the URL"))
}
