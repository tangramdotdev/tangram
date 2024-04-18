use crate::{
	self as tg,
	util::http::{full, Outgoing},
};
use bytes::{Buf, Bytes};
use futures::{future::BoxFuture, FutureExt as _, TryStreamExt as _};
use http_body_util::{BodyExt as _, StreamBody};
use num::ToPrimitive;
use std::{io::Cursor, pin::Pin};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt as _, AsyncSeek};
use tokio_util::io::ReaderStream;

/// A blob kind.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Leaf,
	Branch,
}

/// A blob ID.
#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub enum Id {
	Leaf(tg::leaf::Id),
	Branch(tg::branch::Id),
}

#[derive(Clone, Debug, derive_more::From)]
pub enum Blob {
	Leaf(tg::Leaf),
	Branch(tg::Branch),
}

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum CompressionFormat {
	Bz2,
	Gz,
	Xz,
	Zstd,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CompressArg {
	pub format: CompressionFormat,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CompressOutput {
	pub id: Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DecompressArg {
	pub format: CompressionFormat,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DecompressOutput {
	pub id: Id,
}

/// A blob reader.
pub struct Reader<H> {
	blob: Blob,
	cursor: Option<Cursor<Bytes>>,
	position: u64,
	read: Option<BoxFuture<'static, tg::Result<Option<Cursor<Bytes>>>>>,
	size: u64,
	tg: H,
}

impl Blob {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		match id {
			Id::Leaf(id) => tg::Leaf::with_id(id).into(),
			Id::Branch(id) => tg::Branch::with_id(id).into(),
		}
	}

	pub async fn id<H>(&self, tg: &H, transaction: Option<&H::Transaction<'_>>) -> tg::Result<Id>
	where
		H: tg::Handle,
	{
		match self {
			Self::Leaf(leaf) => Ok(leaf.id(tg, transaction).await?.into()),
			Self::Branch(branch) => Ok(branch.id(tg, transaction).await?.into()),
		}
	}
}

impl Blob {
	/// Create a [`Blob`] from an `AsyncRead`.
	#[must_use]
	pub fn new(children: Vec<tg::branch::Child>) -> Self {
		match children.len() {
			0 => Self::default(),
			1 => children.into_iter().next().unwrap().blob,
			_ => tg::Branch::new(children).into(),
		}
	}

	pub async fn with_reader<H>(
		tg: &H,
		reader: impl AsyncRead + Send + 'static,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let id = tg.create_blob(reader, transaction).boxed().await?;
		let blob = Self::with_id(id);
		Ok(blob)
	}

	pub async fn size(&self, tg: &impl tg::Handle) -> tg::Result<u64> {
		match self {
			Self::Leaf(leaf) => {
				let bytes = &leaf.bytes(tg).await?;
				let size = bytes.len().to_u64().unwrap();
				Ok(size)
			},
			Self::Branch(branch) => {
				let children = &branch.children(tg).await?;
				let size = children.iter().map(|child| child.size).sum();
				Ok(size)
			},
		}
	}

	pub async fn reader<H>(&self, tg: &H) -> tg::Result<Reader<H>>
	where
		H: tg::Handle,
	{
		Reader::new(tg, self.clone()).await
	}

	pub async fn bytes(&self, tg: &impl tg::Handle) -> tg::Result<Vec<u8>> {
		let mut reader = self.reader(tg).await?;
		let mut bytes = Vec::new();
		reader
			.read_to_end(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
		Ok(bytes)
	}

	pub async fn text(&self, tg: &impl tg::Handle) -> tg::Result<String> {
		let bytes = self.bytes(tg).await?;
		let string = String::from_utf8(bytes)
			.map_err(|source| tg::error!(!source, "failed to decode the blob's bytes as UTF-8"))?;
		Ok(string)
	}

	pub async fn compress(
		&self,
		tg: &impl tg::Handle,
		format: CompressionFormat,
	) -> tg::Result<Self> {
		let id = self.id(tg, None).await?;
		let arg = CompressArg { format };
		let output = tg.compress_blob(&id, arg).await?;
		let blob = Self::with_id(output.id);
		Ok(blob)
	}

	pub async fn decompress(
		&self,
		tg: &impl tg::Handle,
		format: CompressionFormat,
	) -> tg::Result<Self> {
		let id = self.id(tg, None).await?;
		let arg = DecompressArg { format };
		let output = tg.decompress_blob(&id, arg).await?;
		let blob = Self::with_id(output.id);
		Ok(blob)
	}
}

impl tg::Client {
	pub async fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		_transaction: Option<&()>,
	) -> tg::Result<tg::blob::Id> {
		let method = http::Method::POST;
		let uri = "/blobs";
		let body = Outgoing::new(StreamBody::new(
			ReaderStream::new(reader)
				.map_ok(hyper::body::Frame::data)
				.map_err(|source| tg::error!(!source, "failed to read from the reader")),
		));
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}

	pub async fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::CompressArg,
	) -> tg::Result<tg::blob::CompressOutput> {
		let method = http::Method::POST;
		let uri = format!("/blobs/{id}/compress");
		let body = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}

	pub async fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::DecompressArg,
	) -> tg::Result<tg::blob::DecompressOutput> {
		let method = http::Method::POST;
		let uri = format!("/blobs/{id}/decompress");
		let body = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
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
	type Err = tg::Error;

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
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			tg::id::Kind::Leaf => Ok(Self::Leaf(value.try_into()?)),
			tg::id::Kind::Branch => Ok(Self::Branch(value.try_into()?)),
			value => Err(tg::error!(%value, "expected a blob ID")),
		}
	}
}

impl From<Id> for tg::object::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Leaf(id) => id.into(),
			Id::Branch(id) => id.into(),
		}
	}
}

impl TryFrom<tg::object::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::object::Id) -> tg::Result<Self, Self::Error> {
		match value {
			tg::object::Id::Leaf(value) => Ok(value.into()),
			tg::object::Id::Branch(value) => Ok(value.into()),
			value => Err(tg::error!(%value, "expected a blob ID")),
		}
	}
}

impl Default for Blob {
	fn default() -> Self {
		Self::Leaf(tg::Leaf::default())
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

impl From<Blob> for tg::object::Handle {
	fn from(value: Blob) -> Self {
		match value {
			Blob::Leaf(leaf) => Self::Leaf(leaf),
			Blob::Branch(branch) => Self::Branch(branch),
		}
	}
}

impl TryFrom<tg::object::Handle> for Blob {
	type Error = tg::Error;

	fn try_from(value: tg::object::Handle) -> tg::Result<Self, Self::Error> {
		match value {
			tg::object::Handle::Leaf(leaf) => Ok(Self::Leaf(leaf)),
			tg::object::Handle::Branch(branch) => Ok(Self::Branch(branch)),
			_ => Err(tg::error!("expected a blob")),
		}
	}
}

impl From<Blob> for tg::Value {
	fn from(value: Blob) -> Self {
		tg::object::Handle::from(value).into()
	}
}

impl TryFrom<tg::Value> for Blob {
	type Error = tg::Error;

	fn try_from(value: tg::Value) -> tg::Result<Self, Self::Error> {
		tg::object::Handle::try_from(value)
			.map_err(|source| tg::error!(!source, "invalid value"))?
			.try_into()
	}
}

unsafe impl<H> Sync for Reader<H> {}

impl<H> Reader<H>
where
	H: tg::Handle,
{
	pub async fn new(tg: &H, blob: Blob) -> tg::Result<Self> {
		let cursor = None;
		let position = 0;
		let read = None;
		let size = blob.size(tg).await?;
		let tg = tg.clone();
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
			let tg = this.tg.clone();
			let blob = this.blob.clone();
			let position = this.position;
			let read = async move { poll_read_inner(&tg, blob, position).await }.boxed();
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
			let tg = this.tg.clone();
			let blob = this.blob.clone();
			let position = this.position;
			let read = async move { poll_read_inner(&tg, blob, position).await }.boxed();
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

async fn poll_read_inner(
	tg: &impl tg::Handle,
	blob: Blob,
	position: u64,
) -> tg::Result<Option<Cursor<Bytes>>> {
	let mut current_blob = blob.clone();
	let mut current_blob_position = 0;
	'a: loop {
		match current_blob {
			Blob::Leaf(leaf) => {
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
				for child in branch.children(tg).await?.iter() {
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

impl std::fmt::Display for CompressionFormat {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let s = match self {
			Self::Bz2 => "bz2",
			Self::Gz => "gz",
			Self::Xz => "xz",
			Self::Zstd => "zst",
		};
		write!(f, "{s}")?;
		Ok(())
	}
}

impl std::str::FromStr for CompressionFormat {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"bz2" => Ok(Self::Bz2),
			"gz" => Ok(Self::Gz),
			"xz" => Ok(Self::Xz),
			"zst" => Ok(Self::Zstd),
			extension => Err(tg::error!(%extension, "invalid compression format")),
		}
	}
}

impl From<CompressionFormat> for String {
	fn from(value: CompressionFormat) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for CompressionFormat {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self, Self::Error> {
		value.parse()
	}
}
