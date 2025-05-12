use std::sync::Arc;
use tangram_client as tg;
use tokio::sync::Mutex;

mod close;
mod create;
mod read;
mod write;

pub(crate) struct Pipe {
	pub host: Arc<Mutex<tokio::net::UnixStream>>,
	pub guest: std::os::unix::net::UnixStream,
}

impl Pipe {
	async fn open() -> tg::Result<Self> {
		let (host, guest) = tokio::net::UnixStream::pair()
			.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
		let guest = guest
			.into_std()
			.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
		guest
			.set_nonblocking(false)
			.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
		let host = Arc::new(Mutex::new(host));
		Ok(Self { host, guest })
	}
}
