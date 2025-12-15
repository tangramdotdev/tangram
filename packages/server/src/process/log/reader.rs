use {
	crate::Server,
	num::ToPrimitive,
	std::{io::SeekFrom, os::unix::fs::MetadataExt, pin::Pin},
	tangram_client::prelude::*,
	tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _},
};

pub struct Reader {
	inner: tokio::io::BufReader<Inner>,
	buffer: Vec<u8>,
}

enum Inner {
	Blob(crate::read::Reader),
	File(tokio::fs::File),
}

impl Reader {
	pub async fn new(server: &Server, id: &tg::process::Id) -> tg::Result<Self> {
		// Create the inner reader.
		let inner = 'a: {
			let output = server
				.try_get_process_local(id)
				.await?
				.ok_or_else(|| tg::error!("expected the process to exist"))?;

			if let Some(log) = output.data.log {
				let blob = tg::Blob::with_id(log);
				let reader = crate::read::Reader::new(server, blob).await?;
				break 'a Inner::Blob(reader);
			}

			// Attempt to create a file reader.
			let path = server.logs_path().join(format!("{id}"));
			match tokio::fs::File::open(&path).await {
				Ok(file) => {
					break 'a Inner::File(file);
				},
				Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
				Err(source) => {
					return Err(
						tg::error!(!source, path = %path.display(), "failed to open the log file"),
					);
				},
			}
			return Err(tg::error!("failed to find the log"));
		};
		let inner = tokio::io::BufReader::new(inner);
		Ok(Self {
			inner,
			buffer: Vec::with_capacity(4096),
		})
	}

	pub async fn next(
		&mut self,
		reverse: bool,
	) -> tg::Result<Option<tg::process::log::get::Chunk>> {
		let chunk = self.read_chunk().await?;
		if let Some((chunk, size)) = &chunk
			&& reverse
			&& chunk.position > 0
		{
			let shift = -(size.to_i64().unwrap() + 1);
			self.inner
				.seek(SeekFrom::Current(shift))
				.await
				.map_err(|source| tg::error!(!source, "failed to seek the stream"))?;
			self.find_line_start().await?;
		}
		Ok(chunk.map(|c| c.0))
	}

	/// Perform a binary search through the file for the file
	pub async fn seek(&mut self, position: SeekFrom) -> tg::Result<()> {
		// Compute the timestamp we're going to seek to.
		let position = match position {
			SeekFrom::Start(0) => {
				self.inner
					.seek(SeekFrom::Start(0))
					.await
					.map_err(|source| tg::error!(!source, "failed to seek the stream"))?;
				return Ok(());
			},
			SeekFrom::Start(position) => position,
			SeekFrom::Current(0) => return Ok(()),
			SeekFrom::Current(position) => {
				self.find_line_start().await?;
				let Some((chunk, _)) = self.read_chunk().await? else {
					return Ok(());
				};
				(chunk.position.to_i64().unwrap() + position)
					.max(0)
					.to_u64()
					.unwrap()
			},
			SeekFrom::End(position) => {
				self.inner
					.seek(SeekFrom::End(-1))
					.await
					.map_err(|source| tg::error!(!source, "failed to seek"))?;
				if position > 0 {
					return Ok(());
				}
				self.find_line_start().await?;
				let Some((chunk, _)) = self.read_chunk().await? else {
					return Ok(());
				};
				let chunk_end = chunk.position + chunk.bytes.len().to_u64().unwrap();
				(chunk_end.to_i64().unwrap() + position).to_u64().unwrap()
			},
		};

		// Get the file size.
		let mut left = 0;
		let mut right = self.inner.get_ref().length().await?;
		while left < right {
			// Seek to the midpoint.
			let midpoint = left + (right - left) / 2;
			self.inner
				.seek(SeekFrom::Start(midpoint))
				.await
				.map_err(|source| tg::error!(!source, "failed to seek"))?;

			// Find the newline.
			let line_start = self.find_line_start().await?;

			// Read the chunk.
			let Some((chunk, size)) = self.read_chunk().await? else {
				return Ok(());
			};

			// Check if this position is within range of the chunk.
			let range = chunk.position..=(chunk.position + chunk.bytes.len().to_u64().unwrap());
			if range.contains(&position) {
				self.inner
					.seek(SeekFrom::Start(line_start))
					.await
					.map_err(|source| tg::error!(!source, "failed to seek"))?;
				break;
			} else if range.end() < &position {
				left = line_start + size.to_u64().unwrap();
			} else if range.start() > &position {
				right = line_start;
			} else {
				unreachable!();
			}
		}

		Ok(())
	}

	async fn read_chunk(&mut self) -> tg::Result<Option<(tg::process::log::get::Chunk, usize)>> {
		self.buffer.clear();
		let size = self
			.inner
			.read_until(b'\n', &mut self.buffer)
			.await
			.map_err(|source| tg::error!(!source, "failed to read log file"))?;
		if size == 0 {
			return Ok(None);
		}
		self.buffer.truncate(size);
		let chunk = serde_json::from_slice(&self.buffer)
			.map_err(|source| tg::error!(!source, "failed to deserialize the log"))?;
		Ok(Some((chunk, size)))
	}

	async fn find_line_start(&mut self) -> tg::Result<u64> {
		let mut current_position = self
			.inner
			.stream_position()
			.await
			.map_err(|source| tg::error!(!source, "failed to get current stream position"))?;
		let position = 'outer: loop {
			if current_position == 0 {
				break 'outer 0;
			}
			let next_position = current_position.saturating_sub(4096);
			self.inner
				.seek(SeekFrom::Start(next_position))
				.await
				.map_err(|source| tg::error!(!source, "failed to seek the log file"))?;
			let size = (current_position - next_position).to_usize().unwrap();
			self.buffer.clear();
			self.buffer.resize(size, 0);
			let size = self
				.inner
				.read(&mut self.buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the log file"))?;
			self.buffer.truncate(size);
			for n in (0..self.buffer.len()).rev() {
				if self.buffer[n] == b'\n' {
					break 'outer next_position + n.to_u64().unwrap() + 1;
				}
			}
			current_position = next_position;
		};
		self.inner
			.seek(SeekFrom::Start(position))
			.await
			.map_err(|source| tg::error!(!source, "failed to seek the log file"))?;
		Ok(position)
	}
}

impl Inner {
	async fn length(&self) -> tg::Result<u64> {
		match self {
			Inner::Blob(blob) => Ok(blob.length()),
			Inner::File(file) => file
				.metadata()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the log file metadata"))
				.map(|metadata| metadata.size()),
		}
	}
}

impl AsyncRead for Inner {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match self.get_mut() {
			Inner::Blob(reader) => Pin::new(reader).poll_read(cx, buf),
			Inner::File(reader) => Pin::new(reader).poll_read(cx, buf),
		}
	}
}

impl AsyncSeek for Inner {
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		match self.get_mut() {
			Inner::Blob(reader) => Pin::new(reader).start_seek(position),
			Inner::File(reader) => Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Inner::Blob(reader) => Pin::new(reader).poll_complete(cx),
			Inner::File(reader) => Pin::new(reader).poll_complete(cx),
		}
	}
}
