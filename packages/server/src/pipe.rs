use tangram_client as tg;

mod close;
mod create;
mod read;
mod write;

pub(crate) struct Pipe {
	pub read: std::os::unix::net::UnixStream,
	pub write: std::os::unix::net::UnixStream,
}

impl Pipe {
	async fn open() -> tg::Result<Self> {
		let (read, write) = std::os::unix::net::UnixStream::pair()
			.map_err(|source| tg::error!(!source, "failed to create pipes"))?;
		Ok(Self { read, write })
	}
}
