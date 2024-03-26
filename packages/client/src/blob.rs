use crate::{branch, id, leaf, object, Artifact, Branch, Handle, Leaf, Result, Value};
use bytes::Bytes;
use derive_more::From;
use futures::{
	future::BoxFuture,
	stream::{self, StreamExt},
	FutureExt, TryStreamExt,
};
use num::ToPrimitive;
use std::{io::Cursor, pin::Pin};
use tangram_error::{error, Error};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};
use tokio_util::io::SyncIoBridge;

const MAX_BRANCH_CHILDREN: usize = 1024;

const MIN_LEAF_SIZE: u32 = 4096;
const AVG_LEAF_SIZE: u32 = 16_384;
const MAX_LEAF_SIZE: u32 = 65_536;

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

	/// Create a [`Blob`] from an implementer of `AsyncRead`.
	pub async fn with_reader(tg: &dyn Handle, reader: impl AsyncRead + Unpin) -> Result<Self> {
		let mut chunker = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);
		let stream = chunker.as_stream();
		let mut children = stream
			.map(|chunk| {
				let bytes = chunk
					.map_err(|source| error!(!source, "failed to read data"))?
					.data;
				let size = bytes.len().to_u64().unwrap();
				let leaf = Leaf::new(bytes.into());
				Ok::<_, tangram_error::Error>(branch::Child {
					blob: leaf.into(),
					size,
				})
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Create the tree.
		while children.len() > MAX_BRANCH_CHILDREN {
			children = stream::iter(children)
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
		let blob = Self::new(children);

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
			.map_err(|source| error!(!source, "failed to read the blob"))?;
		Ok(bytes)
	}

	pub async fn text(&self, tg: &dyn Handle) -> Result<String> {
		let bytes = self.bytes(tg).await?;
		let string = String::from_utf8(bytes).map_err(|error| {
			error!(source = error, "failed to decode the blob's bytes as UTF-8")
		})?;
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

		// Create a temporary path.
		let tempdir = tempfile::TempDir::new()
			.map_err(|source| error!(!source, "failed to create the temporary directory"))?;
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
						archive.unpack(path).map_err(|error| {
							error!(source = error, "failed to extract the archive")
						})?;
					},
					ArchiveFormat::Zip => {
						let mut archive = zip::ZipArchive::new(reader).map_err(|error| {
							error!(source = error, "failed to extract the archive")
						})?;
						archive.extract(&path).map_err(|error| {
							error!(source = error, "failed to extract the archive")
						})?;
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
			.map_err(|source| error!(!source, "failed to check in the extracted archive"))?;

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
			value => Err(error!(%value, "expected a blob ID")),
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
			value => Err(error!(%value, "expected a blob ID")),
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

impl From<Blob> for object::Handle {
	fn from(value: Blob) -> Self {
		match value {
			Blob::Leaf(leaf) => Self::Leaf(leaf),
			Blob::Branch(branch) => Self::Branch(branch),
		}
	}
}

impl TryFrom<object::Handle> for Blob {
	type Error = Error;

	fn try_from(value: object::Handle) -> Result<Self, Self::Error> {
		match value {
			object::Handle::Leaf(leaf) => Ok(Self::Leaf(leaf)),
			object::Handle::Branch(branch) => Ok(Self::Branch(branch)),
			object => Err(error!(%object, "expected a blob")),
		}
	}
}

impl From<Blob> for Value {
	fn from(value: Blob) -> Self {
		object::Handle::from(value).into()
	}
}

impl TryFrom<Value> for Blob {
	type Error = Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		object::Handle::try_from(value)
			.map_err(|source| error!(!source, "invalid value"))?
			.try_into()
	}
}

/// A blob reader.
pub struct Reader {
	blob: Blob,
	cursor: Option<Cursor<Bytes>>,
	position: u64,
	read: Option<BoxFuture<'static, Result<Option<Cursor<Bytes>>>>>,
	size: u64,
	tg: Box<dyn Handle>,
}

unsafe impl Sync for Reader {}

impl Reader {
	pub async fn new(tg: &dyn Handle, blob: Blob) -> Result<Self> {
		let cursor = None;
		let position = 0;
		let read = None;
		let size = blob.size(tg).await?;
		let tg = tg.clone_box();
		Ok(Self {
			blob,
			cursor,
			position,
			read,
			size,
			tg,
		})
	}

	pub fn position(&self) -> u64 {
		self.position
	}

	pub fn end(&self) -> bool {
		self.position == self.size
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let tg = this.tg.clone_box();
			let blob = this.blob.clone();
			let position = this.position;
			let read = async move { poll_read_inner(tg, blob, position).await }.boxed();
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

async fn poll_read_inner(
	tg: Box<dyn Handle>,
	blob: Blob,
	position: u64,
) -> Result<Option<Cursor<Bytes>>> {
	let mut current_blob = blob.clone();
	let mut current_blob_position = 0;
	'a: loop {
		match current_blob {
			Blob::Leaf(leaf) => {
				let (id, object) = {
					let state = leaf.state().read().unwrap();
					(state.id.clone(), state.object.clone())
				};
				let bytes = if let Some(object) = object {
					object.bytes.clone()
				} else {
					tg.get_object(&id.unwrap().into()).await?.bytes.clone()
				};
				if position < current_blob_position + bytes.len().to_u64().unwrap() {
					let mut cursor = Cursor::new(bytes.clone());
					cursor.set_position(position - current_blob_position);
					break Ok(Some(cursor));
				}
				return Ok(None);
			},
			Blob::Branch(branch) => {
				for child in branch.children(tg.as_ref()).await? {
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
			extension => Err(error!(%extension, "invalid format")),
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
			extension => Err(error!(%extension, "invalid compression format")),
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
