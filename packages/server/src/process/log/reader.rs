use crate::Server;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncSeek};

pub enum Reader {
	Blob(crate::blob::Reader),
	File(tokio::fs::File),
}

impl Reader {
	pub async fn new(server: &Server, id: &tg::process::Id) -> tg::Result<Self> {
		// Attempt to create a blob reader.
		let output = server
			.try_get_process_local(id)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;
		if let Some(log) = output.data.log {
			let blob = tg::Blob::with_id(log);
			let reader = crate::blob::Reader::new(server, blob).await?;
			return Ok(Self::Blob(reader));
		}

		// Attempt to create a file reader.
		let path = server.logs_path().join(format!("{id}"));
		match tokio::fs::File::open(&path).await {
			Ok(file) => {
				return Ok(Self::File(file));
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(source) => {
				return Err(
					tg::error!(!source, %path = path.display(), "failed to open the log file"),
				);
			},
		}

		Err(tg::error!("failed to find the log"))
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
			Reader::File(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
		}
	}
}

impl AsyncSeek for Reader {
	fn start_seek(
		self: std::pin::Pin<&mut Self>,
		position: std::io::SeekFrom,
	) -> std::io::Result<()> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).start_seek(position),
			Reader::File(reader) => std::pin::Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).poll_complete(cx),
			Reader::File(reader) => std::pin::Pin::new(reader).poll_complete(cx),
		}
	}
}
