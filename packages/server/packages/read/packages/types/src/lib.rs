use {
	bytes::{Buf as _, Bytes},
	futures::future::BoxFuture,
	num::ToPrimitive as _,
	std::{
		io::{BufRead, Cursor, Read, Seek},
		pin::Pin,
		task::Poll,
	},
	sync_wrapper::SyncWrapper,
	tangram_client::prelude::*,
	tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncRead, AsyncSeek, AsyncSeekExt as _},
};

pub type ReadFuture = BoxFuture<'static, tg::Result<Option<Cursor<Bytes>>>>;

/// A loader that reads the leaf of a blob that contains a position.
pub trait Loader: Send + Sync + 'static {
	fn read(&self, blob: tg::Blob, position: u64) -> ReadFuture;

	fn read_sync(&mut self, blob: &tg::Blob, position: u64) -> tg::Result<Option<Cursor<Bytes>>>;
}

pub enum Reader {
	File(File),
	Object(Object),
}

pub struct File {
	current: u64,
	length: u64,
	position: u64,
	reader: tg::Either<tokio::io::BufReader<tokio::fs::File>, std::io::BufReader<std::fs::File>>,
	seeking: bool,
}

pub struct Object {
	blob: tg::Blob,
	cursor: Option<Cursor<Bytes>>,
	length: u64,
	loader: Box<dyn Loader>,
	position: u64,
	read: Option<SyncWrapper<ReadFuture>>,
}

impl File {
	pub async fn new(file: tokio::fs::File, position: u64, length: u64) -> tg::Result<Self> {
		let mut reader = tokio::io::BufReader::new(file);
		reader
			.seek(std::io::SeekFrom::Start(position))
			.await
			.map_err(|error| tg::error!(!error, "failed to seek the file"))?;
		Ok(Self {
			current: 0,
			length,
			position,
			reader: tg::Either::Left(reader),
			seeking: false,
		})
	}

	pub fn new_sync(file: std::fs::File, position: u64, length: u64) -> tg::Result<Self> {
		let mut reader = std::io::BufReader::new(file);
		reader
			.seek(std::io::SeekFrom::Start(position))
			.map_err(|error| tg::error!(!error, "failed to seek the file"))?;
		Ok(Self {
			current: 0,
			length,
			position,
			reader: tg::Either::Right(reader),
			seeking: false,
		})
	}
}

impl Object {
	#[must_use]
	pub fn new(blob: tg::Blob, length: u64, loader: Box<dyn Loader>) -> Self {
		Self {
			blob,
			cursor: None,
			length,
			loader,
			position: 0,
			read: None,
		}
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).poll_read(cx, buf),
			Reader::Object(reader) => Pin::new(reader).poll_read(cx, buf),
		}
	}
}

impl AsyncBufRead for Reader {
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<&[u8]>> {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).poll_fill_buf(cx),
			Reader::Object(reader) => Pin::new(reader).poll_fill_buf(cx),
		}
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).consume(amt),
			Reader::Object(reader) => Pin::new(reader).consume(amt),
		}
	}
}

impl AsyncSeek for Reader {
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).start_seek(position),
			Reader::Object(reader) => Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).poll_complete(cx),
			Reader::Object(reader) => Pin::new(reader).poll_complete(cx),
		}
	}
}

impl Read for Reader {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		match self {
			Reader::File(reader) => Read::read(reader, buf),
			Reader::Object(reader) => Read::read(reader, buf),
		}
	}
}

impl BufRead for Reader {
	fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
		match self {
			Reader::File(reader) => BufRead::fill_buf(reader),
			Reader::Object(reader) => BufRead::fill_buf(reader),
		}
	}

	fn consume(&mut self, amt: usize) {
		match self {
			Reader::File(reader) => BufRead::consume(reader, amt),
			Reader::Object(reader) => BufRead::consume(reader, amt),
		}
	}
}

impl Seek for Reader {
	fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
		match self {
			Reader::File(reader) => Seek::seek(reader, position),
			Reader::Object(reader) => Seek::seek(reader, position),
		}
	}
}

impl AsyncRead for File {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		if this.current >= this.length {
			return Poll::Ready(Ok(()));
		}
		let old = buf.filled().len();
		let poll = Pin::new(this.reader.as_mut().unwrap_left()).poll_read(cx, buf);
		if let Poll::Ready(Ok(())) = poll {
			let new = buf.filled().len();
			let diff = (new - old).min((this.length - this.current).to_usize().unwrap());
			buf.set_filled(old + diff);
			this.current += diff.to_u64().unwrap();
			return Poll::Ready(Ok(()));
		}
		poll
	}
}

impl AsyncBufRead for File {
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<&[u8]>> {
		let this = self.get_mut();
		if this.current >= this.length {
			return Poll::Ready(Ok(&[]));
		}
		let poll = Pin::new(this.reader.as_mut().unwrap_left()).poll_fill_buf(cx);
		if let Poll::Ready(Ok(mut buf)) = poll {
			let n = buf.len();
			let n = n.min((this.length - this.current).to_usize().unwrap());
			buf = &buf[..n];
			return Poll::Ready(Ok(buf));
		}
		poll
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		let this = self.get_mut();
		let amt = amt.min((this.length - this.current).to_usize().unwrap());
		this.current += amt.to_u64().unwrap();
		this.reader.as_mut().unwrap_left().consume(amt);
	}
}

impl AsyncSeek for File {
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.seeking = true;
		let position = match position {
			std::io::SeekFrom::Start(n) => {
				let n = this.position + n;
				std::io::SeekFrom::Start(n)
			},
			std::io::SeekFrom::End(n) => {
				let n = (this.position.to_i64().unwrap() + this.length.to_i64().unwrap() + n)
					.to_u64()
					.ok_or_else(|| std::io::Error::other("invalid seek"))?;
				std::io::SeekFrom::Start(n)
			},
			std::io::SeekFrom::Current(n) => std::io::SeekFrom::Current(n),
		};
		Pin::new(this.reader.as_mut().unwrap_left()).start_seek(position)
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		let this = self.get_mut();
		let poll = Pin::new(this.reader.as_mut().unwrap_left()).poll_complete(cx);
		match poll {
			Poll::Ready(Ok(n)) => {
				if !this.seeking {
					return Poll::Ready(Ok(0));
				}
				this.seeking = false;
				this.current = n - this.position;
				Poll::Ready(Ok(this.current))
			},
			Poll::Ready(Err(error)) => {
				this.seeking = false;
				Poll::Ready(Err(error))
			},
			Poll::Pending => Poll::Pending,
		}
	}
}

impl Read for File {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		if self.current >= self.length {
			return Ok(0);
		}
		let n = self.reader.as_mut().unwrap_right().read(buf)?;
		let n = n.min((self.length - self.current).to_usize().unwrap());
		self.current += n.to_u64().unwrap();
		Ok(n)
	}
}

impl BufRead for File {
	fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
		if self.current >= self.length {
			return Ok(&[]);
		}
		let buf = self.reader.as_mut().unwrap_right().fill_buf()?;
		let n = buf
			.len()
			.min((self.length - self.current).to_usize().unwrap());
		Ok(&buf[..n])
	}

	fn consume(&mut self, amt: usize) {
		let amt = amt.min((self.length - self.current).to_usize().unwrap());
		self.current += amt.to_u64().unwrap();
		self.reader.as_mut().unwrap_right().consume(amt);
	}
}

impl Seek for File {
	fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
		let position = match position {
			std::io::SeekFrom::Start(n) => {
				let n = self.position + n;
				std::io::SeekFrom::Start(n)
			},
			std::io::SeekFrom::End(n) => {
				let n = (self.position.to_i64().unwrap() + self.length.to_i64().unwrap() + n)
					.to_u64()
					.ok_or_else(|| std::io::Error::other("invalid seek"))?;
				std::io::SeekFrom::Start(n)
			},
			std::io::SeekFrom::Current(n) => std::io::SeekFrom::Current(n),
		};
		let n = self.reader.as_mut().unwrap_right().seek(position)?;
		self.current = n - self.position;
		Ok(self.current)
	}
}

impl AsyncRead for Object {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let blob = this.blob.clone();
			let read = SyncWrapper::new(this.loader.read(blob, this.position));
			this.read.replace(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.get_mut().as_mut().poll(cx) {
				Poll::Pending => {
					return Poll::Pending;
				},
				Poll::Ready(Err(error)) => {
					this.read.take();
					return Poll::Ready(Err(std::io::Error::other(error)));
				},
				Poll::Ready(Ok(None)) => {
					this.read.take();
					return Poll::Ready(Ok(()));
				},
				Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			}
		}

		// Read.
		let cursor = this.cursor.as_mut().unwrap();
		let bytes = cursor.get_ref();
		let position = cursor.position().to_usize().unwrap();
		let n = std::cmp::min(buf.remaining(), bytes.len() - position);
		buf.put_slice(&bytes[position..position + n]);
		this.position += n as u64;
		let position = position + n;
		cursor.set_position(position as u64);
		if position == cursor.get_ref().len() {
			this.cursor.take();
		}

		Poll::Ready(Ok(()))
	}
}

impl AsyncBufRead for Object {
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<&[u8]>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let blob = this.blob.clone();
			let read = SyncWrapper::new(this.loader.read(blob, this.position));
			this.read.replace(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.get_mut().as_mut().poll(cx) {
				Poll::Pending => {
					return Poll::Pending;
				},
				Poll::Ready(Err(error)) => {
					this.read.take();
					return Poll::Ready(Err(std::io::Error::other(error)));
				},
				Poll::Ready(Ok(None)) => {
					this.read.take();
					return Poll::Ready(Ok(&[]));
				},
				Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			}
		}

		// Read.
		let cursor = this.cursor.as_ref().unwrap();
		let bytes = &cursor.get_ref()[cursor.position().to_usize().unwrap()..];

		Poll::Ready(Ok(bytes))
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		let this = self.get_mut();
		this.position += amt.to_u64().unwrap();
		let cursor = this.cursor.as_mut().unwrap();
		cursor.advance(amt);
		let empty = cursor.position() == cursor.get_ref().len().to_u64().unwrap();
		if empty {
			this.cursor.take();
		}
	}
}

impl AsyncSeek for Object {
	fn start_seek(self: Pin<&mut Self>, seek: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.read.take();
		let position = match seek {
			std::io::SeekFrom::Start(seek) => seek.to_i64().unwrap(),
			std::io::SeekFrom::End(seek) => this.length.to_i64().unwrap() + seek,
			std::io::SeekFrom::Current(seek) => this.position.to_i64().unwrap() + seek,
		};
		let position = position.to_u64().ok_or(std::io::Error::other(
			"attempted to seek to a negative or overflowing position",
		))?;
		if position > this.length {
			return Err(std::io::Error::other(
				"attempted to seek to a position beyond the end",
			));
		}
		if let Some(cursor) = this.cursor.as_mut() {
			let leaf_position = position.to_i64().unwrap()
				- (this.position.to_i64().unwrap() - cursor.position().to_i64().unwrap());
			if leaf_position >= 0 && leaf_position < cursor.get_ref().len().to_i64().unwrap() {
				cursor.set_position(leaf_position.to_u64().unwrap());
			} else {
				this.cursor.take();
			}
		}
		this.position = position;
		Ok(())
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		Poll::Ready(Ok(self.position))
	}
}

impl Read for Object {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		// Create the cursor if necessary.
		if self.cursor.is_none() && self.read.is_none() {
			let cursor = self
				.loader
				.read_sync(&self.blob, self.position)
				.map_err(std::io::Error::other)?;
			if let Some(cursor) = cursor {
				self.cursor.replace(cursor);
			} else {
				return Ok(0);
			}
		}

		// Read.
		let cursor = self
			.cursor
			.as_mut()
			.ok_or_else(|| std::io::Error::other("failed to get cursor"))?;
		let bytes = cursor.get_ref();
		let position = cursor.position().to_usize().unwrap();
		let n = std::cmp::min(buf.len(), bytes.len() - position);
		buf[..n].copy_from_slice(&bytes[position..position + n]);
		self.position += n as u64;
		let position = position + n;
		cursor.set_position(position as u64);
		if position == cursor.get_ref().len() {
			self.cursor.take();
		}

		Ok(n)
	}
}

impl BufRead for Object {
	fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
		// Create the cursor if necessary.
		if self.cursor.is_none() && self.read.is_none() {
			let cursor = self
				.loader
				.read_sync(&self.blob, self.position)
				.map_err(std::io::Error::other)?;
			if let Some(cursor) = cursor {
				self.cursor.replace(cursor);
			} else {
				return Ok(&[]);
			}
		}

		// Read.
		let cursor = self
			.cursor
			.as_ref()
			.ok_or_else(|| std::io::Error::other("failed to get cursor"))?;
		let bytes = &cursor.get_ref()[cursor.position().to_usize().unwrap()..];

		Ok(bytes)
	}

	fn consume(&mut self, amt: usize) {
		self.position += amt.to_u64().unwrap();
		let cursor = self.cursor.as_mut().unwrap();
		cursor.advance(amt);
		let empty = cursor.position() == cursor.get_ref().len().to_u64().unwrap();
		if empty {
			self.cursor.take();
		}
	}
}

impl Seek for Object {
	fn seek(&mut self, seek: std::io::SeekFrom) -> std::io::Result<u64> {
		self.read.take();
		let position = match seek {
			std::io::SeekFrom::Start(seek) => seek.to_i64().unwrap(),
			std::io::SeekFrom::End(seek) => self.length.to_i64().unwrap() + seek,
			std::io::SeekFrom::Current(seek) => self.position.to_i64().unwrap() + seek,
		};
		let position = position.to_u64().ok_or(std::io::Error::other(
			"attempted to seek to a negative or overflowing position",
		))?;
		if position > self.length {
			return Err(std::io::Error::other(
				"attempted to seek to a position beyond the end",
			));
		}
		if let Some(cursor) = self.cursor.as_mut() {
			let leaf_position = position.to_i64().unwrap()
				- (self.position.to_i64().unwrap() - cursor.position().to_i64().unwrap());
			if leaf_position >= 0 && leaf_position < cursor.get_ref().len().to_i64().unwrap() {
				cursor.set_position(leaf_position.to_u64().unwrap());
			} else {
				self.cursor.take();
			}
		}
		self.position = position;
		Ok(self.position)
	}
}
