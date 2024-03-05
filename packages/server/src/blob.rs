use crate::{database, Server};
use bytes::{Buf, Bytes};
use futures::{future::BoxFuture, stream, FutureExt, StreamExt, TryStreamExt};
use num::ToPrimitive;
use std::{io::Cursor, pin::Pin};
use tangram_client as tg;
use tangram_error::{Error, Result, WrapErr};
use tg::Handle;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};

const MAX_BRANCH_CHILDREN: usize = 1024;

const MAX_LEAF_SIZE: usize = 262_144;

impl Server {
	/// Create a blob from a reader in a single transaction.
	pub async fn create_blob_with_reader(
		&self,
		reader: impl AsyncRead + Unpin,
	) -> Result<tg::Blob> {
		let mut connection = self.inner.database.get().await?;
		let txn = connection.transaction().await?;
		let id = self
			.create_blob_with_reader_and_transaction(reader, &txn)
			.await?;
		txn.commit().await?;
		Ok(tg::Blob::with_id(id))
	}

	/// Create a blob from a reader using an existing transaction.
	pub async fn create_blob_with_reader_and_transaction(
		&self,
		mut reader: impl AsyncRead + Unpin,
		txn: &database::Transaction<'_>,
	) -> Result<tg::blob::Id> {
		let mut leaves = Vec::new();
		let mut bytes = vec![0u8; MAX_LEAF_SIZE];
		loop {
			// Read up to `MAX_LEAF_BLOCK_DATA_SIZE` bytes from the reader.
			let mut position = 0;
			loop {
				let n = reader
					.read(&mut bytes[position..])
					.await
					.wrap_err("Failed to read from the reader.")?;
				position += n;
				if n == 0 || position == bytes.len() {
					break;
				}
			}
			if position == 0 {
				break;
			}
			let size = position.to_u64().unwrap();

			// Create the leaf.
			let bytes = Bytes::copy_from_slice(&bytes[..position]);
			let data = tg::leaf::Data { bytes };
			let id = tg::leaf::Id::new(&data.serialize()?);
			self.put_complete_object_with_transaction(id.clone().into(), data.into(), txn)
				.await?;

			// Add to the tree.
			leaves.push(tg::branch::child::Data {
				blob: id.into(),
				size,
			});
		}

		// Create the tree.
		while leaves.len() > MAX_BRANCH_CHILDREN {
			leaves = stream::iter(leaves)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(async move {
							let size = chunk.len().to_u64().unwrap();
							let data = tg::branch::Data { children: chunk };
							let id = tg::branch::Id::new(&data.serialize()?);
							self.put_complete_object_with_transaction(
								id.clone().into(),
								data.into(),
								txn,
							)
							.await?;
							Ok::<_, Error>(tg::branch::child::Data {
								blob: id.into(),
								size,
							})
						})
						.boxed_local()
					} else {
						stream::iter(chunk.into_iter().map(Result::Ok)).boxed_local()
					}
				})
				.try_collect()
				.await?;
		}

		// Create the blob.
		let data = tg::branch::Data { children: leaves };
		let id = tg::branch::Id::new(&data.serialize()?);
		self.put_complete_object_with_transaction(id.clone().into(), data.into(), txn)
			.await?;
		Ok(id.into())
	}
}

pub struct Reader {
	blob: tg::Blob,
	cursor: Option<Cursor<Bytes>>,
	position: u64,
	read: Option<BoxFuture<'static, Result<Option<Cursor<Bytes>>>>>,
	server: crate::Server,
	size: u64,
}

unsafe impl Sync for Reader {}

impl Reader {
	pub async fn new(server: &Server, blob: tg::Blob) -> Result<Self> {
		let size = blob.size(server).await?;
		let cursor = None;
		let position = 0;
		let server = server.clone();
		let read = None;
		Ok(Self {
			blob,
			cursor,
			position,
			read,
			server,
			size,
		})
	}

	pub async fn get_bytes(&self) -> Result<Option<Bytes>> {
		get_bytes_inner(&self.server, self.blob.clone(), self.position).await
	}
}

impl AsyncSeek for Reader {
	fn start_seek(self: Pin<&mut Self>, seek: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.read.take();
		let position = match seek {
			std::io::SeekFrom::Start(seek) => seek.to_i64().unwrap(),
			std::io::SeekFrom::End(seek) => this.size.to_i64().unwrap() + seek,
			std::io::SeekFrom::Current(seek) => this.position.to_i64().unwrap() + seek,
		};
		let position = position.to_u64().ok_or(std::io::Error::other(
			"Attempted to seek to a negative or overflowing position.",
		))?;
		if position > this.size {
			return Err(std::io::Error::other(
				"Attempted to seek to a position beyond the end.",
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

impl AsyncRead for Reader {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let server = this.server.clone();
			let blob = this.blob.clone();
			let position = this.position;
			let read = async move {
				let bytes = get_bytes_inner(&server, blob, position).await?;
				let cursor = bytes.map(Cursor::new);
				Ok(cursor)
			}
			.boxed();
			this.read.replace(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.as_mut().poll(cx) {
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

// Get the bytes for a blob at an offset using a database connection. This is a free function to avoid excess cloning in the implementation of AsyncRead.
async fn get_bytes_inner(
	server: &crate::Server,
	mut blob: tg::Blob,
	position: u64,
) -> Result<Option<Bytes>> {
	let mut current_blob_position = 0;
	'a: loop {
		match blob {
			tg::Blob::Leaf(leaf) => {
				let (id, object) = {
					let state = leaf.state().read().unwrap();
					(state.id.clone(), state.object.clone())
				};
				let mut bytes = if let Some(object) = object {
					object.bytes.clone()
				} else {
					server.get_object(&id.unwrap().into()).await?.bytes
				};
				if position < current_blob_position + bytes.len().to_u64().unwrap() {
					bytes.advance((position - current_blob_position).to_usize().unwrap());
					break Ok(Some(bytes));
				}
				return Ok(None);
			},
			tg::Blob::Branch(branch) => {
				for child in branch.children(server).await? {
					if position < current_blob_position + child.size {
						blob = child.blob.clone();
						continue 'a;
					}
					current_blob_position += child.size;
				}
				return Ok(None);
			},
		}
	}
}
