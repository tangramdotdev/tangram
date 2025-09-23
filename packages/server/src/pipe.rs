use {crate::Server, std::os::fd::OwnedFd, tangram_client as tg};

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

impl Server {
	pub(crate) fn get_pipe_fd(&self, pipe: &tg::pipe::Id, read: bool) -> tg::Result<OwnedFd> {
		let pipe = self
			.pipes
			.get(pipe)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?;
		let end = if read { &pipe.read } else { &pipe.write };
		let fd = end
			.try_clone()
			.map_err(|source| tg::error!(!source, "failed to clone the fd"))?
			.into();
		Ok(fd)
	}
}
