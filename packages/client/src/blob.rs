use crate::{
	branch, id, leaf, object, return_error, Artifact, Branch, Error, Handle, Leaf, Result, Value,
	WrapErr,
};
use bytes::Bytes;
use derive_more::From;
use futures::{
	future::BoxFuture,
	stream::{self, StreamExt},
	TryStreamExt,
};
use num::ToPrimitive;
use pin_project::pin_project;
use std::{io::Cursor, pin::Pin, task::Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};
use tokio_util::io::SyncIoBridge;

const MAX_BRANCH_CHILDREN: usize = 1024;

const MAX_LEAF_SIZE: usize = 262_144;

/// A blob kind.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Leaf,
	Branch,
}

#[derive(
	Clone, Debug, Eq, From, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub enum Id {
	Leaf(leaf::Id),
	Branch(branch::Id),
}

#[derive(Clone, Debug, From)]
pub enum Blob {
	Leaf(Leaf),
	Branch(Branch),
}

#[derive(Clone, Copy, Debug)]
pub enum ArchiveFormat {
	Tar,
	Zip,
}

#[derive(Clone, Copy, Debug)]
pub enum CompressionFormat {
	Bz2,
	Gz,
	Xz,
	Zstd,
}

impl Blob {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Leaf(id) => Leaf::with_id(id).into(),
			Id::Branch(id) => Branch::with_id(id).into(),
		}
	}

	pub async fn id(&self, tg: &dyn Handle) -> Result<Id> {
		match self {
			Self::Leaf(leaf) => Ok(leaf.id(tg).await?.clone().into()),
			Self::Branch(branch) => Ok(branch.id(tg).await?.clone().into()),
		}
	}

	pub async fn with_reader(tg: &dyn Handle, mut reader: impl AsyncRead + Unpin) -> Result<Self> {
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

			// Create, store, and add the leaf.
			let bytes = Bytes::copy_from_slice(&bytes[..position]);
			let leaf = Leaf::new(bytes);
			leaf.store(tg).await?;
			leaves.push(branch::Child {
				blob: leaf.into(),
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
							let blob = Self::new(chunk);
							let size = blob.size(tg).await?;
							Ok::<_, Error>(branch::Child { blob, size })
						})
						.boxed()
					} else {
						stream::iter(chunk.into_iter().map(Result::Ok)).boxed()
					}
				})
				.try_collect()
				.await?;
		}
		let blob = Self::new(leaves);

		Ok(blob)
	}

	#[must_use]
	pub fn new(children: Vec<branch::Child>) -> Self {
		match children.len() {
			0 => Self::default(),
			1 => children.into_iter().next().unwrap().blob,
			_ => Branch::new(children).into(),
		}
	}

	pub async fn size(&self, tg: &dyn Handle) -> Result<u64> {
		match self {
			Self::Leaf(leaf) => Ok(leaf.bytes(tg).await?.len().to_u64().unwrap()),
			Self::Branch(branch) => Ok(branch
				.children(tg)
				.await?
				.iter()
				.map(|child| child.size)
				.sum()),
		}
	}

	pub async fn reader(&self, tg: &dyn Handle) -> Result<Reader> {
		Reader::new(tg, self.clone()).await
	}

	pub async fn bytes(&self, tg: &dyn Handle) -> Result<Vec<u8>> {
		let mut reader = self.reader(tg).await?;
		let mut bytes = Vec::new();
		reader
			.read_to_end(&mut bytes)
			.await
			.wrap_err("Failed to read the blob.")?;
		Ok(bytes)
	}

	pub async fn text(&self, tg: &dyn Handle) -> Result<String> {
		let bytes = self.bytes(tg).await?;
		let string =
			String::from_utf8(bytes).wrap_err("Failed to decode the blob's bytes as UTF-8.")?;
		Ok(string)
	}

	pub async fn compress(&self, tg: &dyn Handle, format: CompressionFormat) -> Result<Blob> {
		let reader = self.reader(tg).await?;
		let reader = tokio::io::BufReader::new(reader);
		let reader: Box<dyn AsyncRead + Unpin> = match format {
			CompressionFormat::Bz2 => {
				Box::new(async_compression::tokio::bufread::BzEncoder::new(reader))
			},
			CompressionFormat::Gz => {
				Box::new(async_compression::tokio::bufread::GzipEncoder::new(reader))
			},
			CompressionFormat::Xz => {
				Box::new(async_compression::tokio::bufread::XzEncoder::new(reader))
			},
			CompressionFormat::Zstd => {
				Box::new(async_compression::tokio::bufread::ZstdEncoder::new(reader))
			},
		};
		let blob = Blob::with_reader(tg, reader).await?;
		Ok(blob)
	}

	pub async fn decompress(&self, tg: &dyn Handle, format: CompressionFormat) -> Result<Blob> {
		let reader = self.reader(tg).await?;
		let reader = tokio::io::BufReader::new(reader);
		let reader: Box<dyn AsyncRead + Unpin> = match format {
			CompressionFormat::Bz2 => {
				Box::new(async_compression::tokio::bufread::BzDecoder::new(reader))
			},
			CompressionFormat::Gz => {
				Box::new(async_compression::tokio::bufread::GzipDecoder::new(reader))
			},
			CompressionFormat::Xz => {
				Box::new(async_compression::tokio::bufread::XzDecoder::new(reader))
			},
			CompressionFormat::Zstd => {
				Box::new(async_compression::tokio::bufread::ZstdDecoder::new(reader))
			},
		};
		let blob = Blob::with_reader(tg, reader).await?;
		Ok(blob)
	}

	#[allow(clippy::unused_async)]
	pub async fn archive(
		_tg: &dyn Handle,
		_artifact: &Artifact,
		_format: ArchiveFormat,
	) -> Result<Self> {
		unimplemented!()
	}

	pub async fn extract(&self, tg: &dyn Handle, format: ArchiveFormat) -> Result<Artifact> {
		// Create the reader.
		let reader = self.reader(tg).await?;

		// Create a temp.
		let tempdir = tempfile::TempDir::new().wrap_err("Failed to create the temporary leaf.")?;
		let path = tempdir.path().join("archive");

		// Extract in a blocking task.
		tokio::task::spawn_blocking({
			let reader = SyncIoBridge::new(reader);
			let path = path.clone();
			move || -> Result<_> {
				match format {
					ArchiveFormat::Tar => {
						let mut archive = tar::Archive::new(reader);
						archive.set_preserve_permissions(false);
						archive.set_unpack_xattrs(false);
						archive
							.unpack(path)
							.wrap_err("Failed to extract the archive.")?;
					},
					ArchiveFormat::Zip => {
						let mut archive = zip::ZipArchive::new(reader)
							.wrap_err("Failed to extract the archive.")?;
						archive
							.extract(&path)
							.wrap_err("Failed to extract the archive.")?;
					},
				}
				Ok(())
			}
		})
		.await
		.unwrap()?;

		// Check in the extracted artifact.
		let path = path.try_into()?;
		let artifact = Artifact::check_in(tg, &path)
			.await
			.wrap_err("Failed to check in the extracted archive.")?;

		Ok(artifact)
	}
}

impl std::fmt::Display for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Leaf(id) => write!(f, "{id}"),
			Self::Branch(id) => write!(f, "{id}"),
		}
	}
}

impl std::str::FromStr for Id {
	type Err = Error;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Leaf(id) => id.into(),
			Id::Branch(id) => id.into(),
		}
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = Error;

	fn try_from(value: crate::Id) -> Result<Self, Self::Error> {
		match value.kind() {
			id::Kind::Leaf => Ok(Self::Leaf(value.try_into()?)),
			id::Kind::Branch => Ok(Self::Branch(value.try_into()?)),
			_ => return_error!("Expected a blob ID."),
		}
	}
}

impl From<Id> for object::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Leaf(id) => id.into(),
			Id::Branch(id) => id.into(),
		}
	}
}

impl TryFrom<object::Id> for Id {
	type Error = Error;

	fn try_from(value: object::Id) -> Result<Self, Self::Error> {
		match value {
			object::Id::Leaf(value) => Ok(value.into()),
			object::Id::Branch(value) => Ok(value.into()),
			_ => return_error!("Expected a blob ID."),
		}
	}
}

impl Default for Blob {
	fn default() -> Self {
		Self::Leaf(Leaf::default())
	}
}

impl std::fmt::Display for Blob {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Leaf(leaf) => write!(f, "{leaf}"),
			Self::Branch(branch) => write!(f, "{branch}"),
		}
	}
}

impl From<Blob> for Value {
	fn from(value: Blob) -> Self {
		match value {
			Blob::Leaf(leaf) => leaf.into(),
			Blob::Branch(branch) => branch.into(),
		}
	}
}

impl TryFrom<Value> for Blob {
	type Error = Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Leaf(leaf) => Ok(Self::Leaf(leaf)),
			Value::Branch(branch) => Ok(Self::Branch(branch)),
			_ => return_error!("Expected a blob."),
		}
	}
}

/// A blob reader.
#[pin_project]
pub struct Reader {
	blob: Blob,
	tg: Box<dyn Handle>,
	position: u64,
	size: u64,
	state: State,
}

pub enum State {
	Empty,
	Reading(BoxFuture<'static, Result<Cursor<Bytes>>>),
	Full(Cursor<Bytes>),
}

unsafe impl Sync for State {}

impl Reader {
	pub async fn new(tg: &dyn Handle, blob: Blob) -> Result<Self> {
		let size = blob.size(tg).await?;
		Ok(Self {
			blob,
			tg: tg.clone_box(),
			position: 0,
			size,
			state: State::Empty,
		})
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.project();
		loop {
			match this.state {
				State::Empty => {
					if *this.position == *this.size {
						return Poll::Ready(Ok(()));
					}
					let future = {
						let blob = this.blob.clone();
						let tg = this.tg.clone_box();
						let position = *this.position;
						async move {
							let mut current_blob = blob.clone();
							let mut current_blob_position = 0;
							let bytes = 'outer: loop {
								match current_blob {
									Blob::Leaf(leaf) => {
										let (id, object) = {
											let state = leaf.state().read().unwrap();
											(state.id.clone(), state.object.clone())
										};
										let bytes = if let Some(object) = object {
											object.bytes.clone()
										} else {
											tg.get_object(&id.unwrap().into()).await?.clone()
										};
										if position
											< current_blob_position + bytes.len().to_u64().unwrap()
										{
											let mut reader = Cursor::new(bytes.clone());
											reader.set_position(position - current_blob_position);
											break reader;
										}
										return_error!("The position is out of bounds.");
									},
									Blob::Branch(branch) => {
										for child in branch.children(tg.as_ref()).await? {
											if position < current_blob_position + child.size {
												current_blob = child.blob.clone();
												continue 'outer;
											}
											current_blob_position += child.size;
										}
										return_error!("The position is out of bounds.");
									},
								}
							};
							Ok(bytes)
						}
					};
					let future = Box::pin(future);
					*this.state = State::Reading(future);
				},

				State::Reading(future) => match future.as_mut().poll(cx) {
					Poll::Pending => return Poll::Pending,
					Poll::Ready(Err(error)) => {
						return Poll::Ready(Err(std::io::Error::new(
							std::io::ErrorKind::Other,
							error,
						)))
					},
					Poll::Ready(Ok(data)) => {
						*this.state = State::Full(data);
					},
				},

				State::Full(reader) => {
					let data = reader.get_ref();
					let position = reader.position().to_usize().unwrap();
					let n = std::cmp::min(buf.remaining(), data.len() - position);
					buf.put_slice(&data[position..position + n]);
					*this.position += n as u64;
					let position = position + n;
					reader.set_position(position as u64);
					if position == reader.get_ref().len() {
						*this.state = State::Empty;
					}
					return Poll::Ready(Ok(()));
				},
			};
		}
	}
}

impl AsyncSeek for Reader {
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.project();
		let position = match position {
			std::io::SeekFrom::Start(position) => position.to_i64().unwrap(),
			std::io::SeekFrom::End(position) => this.size.to_i64().unwrap() + position,
			std::io::SeekFrom::Current(position) => this.position.to_i64().unwrap() + position,
		};
		let position = position.to_u64().ok_or(std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			"Attempted to seek to a negative or overflowing position.",
		))?;
		if position > *this.size {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				"Attempted to seek to a position beyond the end of the blob.",
			));
		}
		*this.state = State::Empty;
		*this.position = position;
		Ok(())
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		Poll::Ready(Ok(self.position))
	}
}

impl std::fmt::Display for ArchiveFormat {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Tar => {
				write!(f, ".tar")?;
			},
			Self::Zip => {
				write!(f, ".zip")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for ArchiveFormat {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			".tar" => Ok(Self::Tar),
			".zip" => Ok(Self::Zip),
			_ => return_error!("Invalid format."),
		}
	}
}

impl From<ArchiveFormat> for String {
	fn from(value: ArchiveFormat) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for ArchiveFormat {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		value.parse()
	}
}

impl std::fmt::Display for CompressionFormat {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let string = match self {
			Self::Bz2 => ".bz2",
			Self::Gz => ".gz",
			Self::Xz => ".xz",
			Self::Zstd => ".zst",
		};
		write!(f, "{string}")?;
		Ok(())
	}
}

impl std::str::FromStr for CompressionFormat {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			".bz2" => Ok(Self::Bz2),
			".gz" => Ok(Self::Gz),
			".xz" => Ok(Self::Xz),
			".zst" | ".zstd" => Ok(Self::Zstd),
			_ => return_error!("Invalid compression format."),
		}
	}
}

impl From<CompressionFormat> for String {
	fn from(value: CompressionFormat) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for CompressionFormat {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		value.parse()
	}
}
