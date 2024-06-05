use crate::{self as tg, Client};
use bytes::{Buf, Bytes};
use futures::{future::BoxFuture, FutureExt as _};
use num::ToPrimitive as _;
use std::{io::Cursor, pin::Pin};
use sync_wrapper::SyncWrapper;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt as _, AsyncSeek};

/// A blob reader.
pub struct Reader<H> {
	blob: tg::Blob,
	cursor: Option<Cursor<Bytes>>,
	handle: H,
	position: u64,
	read: Option<SyncWrapper<ReadFuture>>,
	size: u64,
}

type ReadFuture = BoxFuture<'static, tg::Result<Option<Cursor<Bytes>>>>;

impl tg::Blob {
	pub async fn reader<H>(&self, handle: &H) -> tg::Result<Reader<H>>
	where
		H: tg::Handle,
	{
		Reader::new(handle, self.clone()).await
	}

	pub async fn bytes<H>(&self, handle: &H) -> tg::Result<Vec<u8>>
	where
		H: tg::Handle,
	{
		let mut reader = self.reader(handle).await?;
		let mut bytes = Vec::new();
		reader
			.read_to_end(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
		Ok(bytes)
	}

	pub async fn text<H>(&self, handle: &H) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		let bytes = self.bytes(handle).await?;
		let string = String::from_utf8(bytes)
			.map_err(|source| tg::error!(!source, "failed to decode the blob's bytes as UTF-8"))?;
		Ok(string)
	}
}

impl<H> Reader<H>
where
	H: tg::Handle,
{
	pub async fn new(handle: &H, blob: tg::Blob) -> tg::Result<Self> {
		let cursor = None;
		let position = 0;
		let read = None;
		let size = blob.size(handle).await?;
		let handle = handle.clone();
		Ok(Self {
			blob,
			cursor,
			handle,
			position,
			read,
			size,
		})
	}

	pub fn position(&self) -> u64 {
		self.position
	}

	pub fn end(&self) -> bool {
		self.position == self.size
	}
}

impl<H> AsyncRead for Reader<H>
where
	H: tg::Handle,
{
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let handle = this.handle.clone();
			let blob = this.blob.clone();
			let position = this.position;
			let read = SyncWrapper::new(
				async move { poll_read_inner(&handle, blob, position).await }.boxed(),
			);
			this.read.replace(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.get_mut().as_mut().poll(cx) {
				std::task::Poll::Pending => return std::task::Poll::Pending,
				std::task::Poll::Ready(Err(error)) => {
					this.read.take();
					return std::task::Poll::Ready(Err(std::io::Error::other(error)));
				},
				std::task::Poll::Ready(Ok(None)) => {
					this.read.take();
					return std::task::Poll::Ready(Ok(()));
				},
				std::task::Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			};
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

		std::task::Poll::Ready(Ok(()))
	}
}

impl<H> AsyncBufRead for Reader<H>
where
	H: tg::Handle,
{
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<&[u8]>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let handle = this.handle.clone();
			let blob = this.blob.clone();
			let position = this.position;
			let read = SyncWrapper::new(
				async move { poll_read_inner(&handle, blob, position).await }.boxed(),
			);
			this.read.replace(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.get_mut().as_mut().poll(cx) {
				std::task::Poll::Pending => return std::task::Poll::Pending,
				std::task::Poll::Ready(Err(error)) => {
					this.read.take();
					return std::task::Poll::Ready(Err(std::io::Error::other(error)));
				},
				std::task::Poll::Ready(Ok(None)) => {
					this.read.take();
					return std::task::Poll::Ready(Ok(&[]));
				},
				std::task::Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			};
		}

		// Read.
		let cursor = this.cursor.as_ref().unwrap();
		let bytes = &cursor.get_ref()[cursor.position().to_usize().unwrap()..];

		std::task::Poll::Ready(Ok(bytes))
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

async fn poll_read_inner<H>(
	handle: &H,
	blob: tg::Blob,
	position: u64,
) -> tg::Result<Option<Cursor<Bytes>>>
where
	H: tg::Handle,
{
	let mut current_blob = blob.clone();
	let mut current_blob_position = 0;
	'a: loop {
		match current_blob {
			tg::Blob::Leaf(leaf) => {
				let (id, object) = {
					let state = leaf.state().read().unwrap();
					(
						state.id.clone(),
						state.object.as_ref().map(|object| object.as_ref().clone()),
					)
				};
				let bytes = if let Some(object) = object {
					object.bytes.clone()
				} else {
					handle.get_object(&id.unwrap().into()).await?.bytes.clone()
				};
				if position < current_blob_position + bytes.len().to_u64().unwrap() {
					let mut cursor = Cursor::new(bytes.clone());
					cursor.set_position(position - current_blob_position);
					break Ok(Some(cursor));
				}
				return Ok(None);
			},
			tg::Blob::Branch(branch) => {
				for child in branch.children(handle).await?.iter() {
					if position < current_blob_position + child.size {
						current_blob = child.blob.clone();
						continue 'a;
					}
					current_blob_position += child.size;
				}
				return Ok(None);
			},
		}
	}
}

impl<H> AsyncSeek for Reader<H>
where
	H: tg::Handle,
{
	fn start_seek(self: Pin<&mut Self>, seek: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.read.take();
		let position = match seek {
			std::io::SeekFrom::Start(seek) => seek.to_i64().unwrap(),
			std::io::SeekFrom::End(seek) => this.size.to_i64().unwrap() + seek,
			std::io::SeekFrom::Current(seek) => this.position.to_i64().unwrap() + seek,
		};
		let position = position.to_u64().ok_or(std::io::Error::other(
			"attempted to seek to a negative or overflowing position",
		))?;
		if position > this.size {
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
	) -> std::task::Poll<std::io::Result<u64>> {
		std::task::Poll::Ready(Ok(self.position))
	}
}

pub struct Arg {
	pub id: tg::blob::Id,
	pub offset: u64,
	pub length: u64,
}

impl Client {
	pub async fn read_blob(&self, _id: &tg::blob::Id, _arg: Arg) -> tg::Result<Bytes> {
		todo!()
	}
}
