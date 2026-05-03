use {crate::Server, std::path::Path, tangram_client::prelude::*};

impl Server {
	#[cfg(target_os = "macos")]
	pub(super) async fn run_create_listener(
		root_path: &Path,
		isolation: tangram_sandbox::Isolation,
	) -> tg::Result<(crate::http::Listener, tangram_uri::Uri)> {
		match isolation {
			tangram_sandbox::Isolation::Container(_) => {
				Err(tg::error!("container isolation is not supported on macos"))
			},
			tangram_sandbox::Isolation::Seatbelt(_) => {
				Self::run_create_unix_listener(root_path).await
			},
			tangram_sandbox::Isolation::Vm(_) => {
				Err(tg::error!("vm isolation is not supported on macos"))
			},
		}
	}

	#[cfg(target_os = "linux")]
	pub(super) async fn run_create_listener(
		root_path: &Path,
		isolation: tangram_sandbox::Isolation,
	) -> tg::Result<(crate::http::Listener, tangram_uri::Uri)> {
		match isolation {
			tangram_sandbox::Isolation::Container(_) => {
				Self::run_create_unix_listener(root_path).await
			},
			tangram_sandbox::Isolation::Seatbelt(_) => {
				Err(tg::error!("seatbelt isolation is not supported on linux"))
			},
			tangram_sandbox::Isolation::Vm(_) => {
				let _ = root_path;
				#[cfg(not(feature = "vsock"))]
				{
					Err(tg::error!("vsock is not enabled"))
				}
				#[cfg(feature = "vsock")]
				{
					let url = format!("http+vsock://{}:0", tangram_sandbox::vm::HOST_VSOCK_CID)
						.parse::<tangram_uri::Uri>()
						.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?;
					let listener = Server::listen(&url)
						.await
						.map_err(|source| tg::error!(!source, "failed to listen"))?;
					let guest_uri = match &listener {
						crate::http::Listener::Vsock(vsock) => {
							let addr = vsock.local_addr().map_err(|source| {
								tg::error!(!source, "failed to get the listener address")
							})?;
							format!("http+vsock://{}:{}", addr.cid(), addr.port())
								.parse::<tangram_uri::Uri>()
								.map_err(|source| {
									tg::error!(source = source, "failed to parse the URL")
								})?
						},
						_ => unreachable!(),
					};
					Ok((listener, guest_uri))
				}
			},
		}
	}

	async fn run_create_unix_listener(
		root_path: &Path,
	) -> tg::Result<(crate::http::Listener, tangram_uri::Uri)> {
		let host_socket_path =
			tangram_sandbox::Sandbox::host_tangram_socket_path_from_root(root_path);
		let guest_socket_path =
			tangram_sandbox::Sandbox::guest_tangram_socket_path_from_root(root_path);
		let max_socket_path_len = if cfg!(target_os = "macos") {
			100
		} else {
			usize::MAX
		};
		let host_socket_path_string = host_socket_path
			.to_str()
			.ok_or_else(|| tg::error!("invalid socket path"))?;
		let guest_socket_path_string = guest_socket_path
			.to_str()
			.ok_or_else(|| tg::error!("invalid socket path"))?;

		tokio::fs::create_dir_all(host_socket_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the host path"))?;
		let url = if host_socket_path_string.len() <= max_socket_path_len {
			tangram_uri::Uri::builder()
				.scheme("http+unix")
				.authority(host_socket_path_string)
				.path("")
				.build()
				.map_err(|source| tg::error!(source = source, "failed to build the socket URL"))?
		} else {
			"http://localhost:0"
				.parse::<tangram_uri::Uri>()
				.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?
		};
		let listener = Server::listen(&url)
			.await
			.map_err(|source| tg::error!(!source, "failed to listen"))?;
		let guest_uri = match &listener {
			crate::http::Listener::Tcp(tcp) => {
				let port = tcp
					.local_addr()
					.map_err(|source| tg::error!(!source, "failed to get the listener address"))?
					.port();
				format!("http://localhost:{port}")
					.parse::<tangram_uri::Uri>()
					.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?
			},
			crate::http::Listener::Unix(_) => tangram_uri::Uri::builder()
				.scheme("http+unix")
				.authority(guest_socket_path_string)
				.path("")
				.build()
				.map_err(|source| tg::error!(source = source, "failed to build the guest URL"))?,
			#[cfg(feature = "vsock")]
			crate::http::Listener::Vsock(vsock) => {
				let addr = vsock
					.local_addr()
					.map_err(|source| tg::error!(!source, "failed to get the listener address"))?;
				format!("http+vsock://{}:{}", addr.cid(), addr.port())
					.parse::<tangram_uri::Uri>()
					.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?
			},
		};
		Ok((listener, guest_uri))
	}
}
