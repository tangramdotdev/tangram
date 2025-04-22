use crate::{Server, temp::Temp};
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use num::ToPrimitive;
use std::{collections::BTreeMap, pin::pin, sync::Arc};
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;
use tokio::io::{AsyncRead, AsyncWriteExt as _};

const MAX_BRANCH_CHILDREN: usize = 1_024;
const MIN_LEAF_SIZE: u32 = 4_096;
const AVG_LEAF_SIZE: u32 = 65_536;
const MAX_LEAF_SIZE: u32 = 131_072;

#[derive(Clone, Debug)]
pub struct Blob {
	pub bytes: Option<Bytes>,
	pub children: Vec<Blob>,
	pub count: u64,
	pub data: Option<tg::blob::Data>,
	pub depth: u64,
	pub id: tg::blob::Id,
	pub length: u64,
	pub position: u64,
	pub size: u64,
	pub weight: u64,
}

pub enum Destination {
	Temp(Temp),
	Store { touched_at: i64 },
}

impl Server {
	pub async fn create_blob(
		&self,
		reader: impl AsyncRead,
	) -> tg::Result<tg::blob::create::Output> {
		// Get the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Create the destination.
		let destination = if self.config.advanced.shared_directory {
			Destination::Temp(Temp::new(self))
		} else {
			Destination::Store { touched_at }
		};

		// Create the blob.
		let blob = self.create_blob_inner(reader, Some(&destination)).await?;
		let blob = Arc::new(blob);

		// Rename the temp file to the cache directory if necessary.
		let cache_reference = if let Destination::Temp(temp) = destination {
			let data = tg::file::Data::Normal {
				contents: blob.id.clone(),
				dependencies: BTreeMap::new(),
				executable: false,
			};
			let id = tg::file::Id::new(&data.serialize()?);
			let path = self.cache_path().join(id.to_string());
			tokio::fs::rename(temp.path(), path)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to rename the file to the blobs directory")
				})?;
			Some(id.into())
		} else {
			None
		};

		// Create the messenger future.
		let messenger_future = async {
			self.blob_create_messenger(&blob, cache_reference.clone(), touched_at)
				.await
		};

		// Create the store future.
		let store_future = async {
			self.blob_create_store(&blob, cache_reference.clone(), touched_at)
				.await
		};

		// Join the messenger, and store futures.
		futures::try_join!(messenger_future, store_future)?;

		// Create the output.
		let output = tg::blob::create::Output {
			blob: blob.id.clone(),
		};

		Ok(output)
	}

	pub(crate) async fn create_blob_inner(
		&self,
		reader: impl AsyncRead,
		destination: Option<&Destination>,
	) -> tg::Result<Blob> {
		// Create the reader.
		let reader = pin!(reader);
		let mut reader = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);
		let stream = reader.as_stream();
		let mut stream = pin!(stream);

		// Open the destination file if necessary.
		let mut file = if let Some(Destination::Temp(temp)) = destination {
			Some(
				tokio::fs::File::create_new(temp.path())
					.await
					.map_err(|source| tg::error!(!source, "failed to create the file"))?,
			)
		} else {
			None
		};

		// Create the leaves and write or store them if necessary.
		let mut blobs = Vec::new();
		while let Some(chunk) = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to read from the reader"))?
		{
			// Create the leaf.
			let blob = self.create_blob_inner_leaf(&chunk).await?;

			// Store the leaf if necessary.
			match destination {
				None => (),
				Some(Destination::Temp(_)) => {
					file.as_mut()
						.unwrap()
						.write_all(&chunk.data)
						.await
						.map_err(|source| tg::error!(!source, "failed to write to the file"))?;
				},
				Some(Destination::Store { touched_at }) => {
					let arg = crate::store::PutArg {
						bytes: Some(chunk.data.into()),
						cache_reference: None,
						id: blob.id.clone().into(),
						touched_at: *touched_at,
					};
					self.store
						.put(arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to store the leaf"))?;
				},
			}

			blobs.push(blob);
		}

		// Flush and close the file if necessary.
		if let Some(mut file) = file {
			file.flush()
				.await
				.map_err(|source| tg::error!(!source, "failed to flush the file"))?;
			drop(file);
		}

		// Create the tree.
		while blobs.len() > MAX_BRANCH_CHILDREN {
			blobs = stream::iter(blobs)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(self.create_blob_inner_branch(chunk)).left_stream()
					} else {
						stream::iter(chunk.into_iter().map(Ok)).right_stream()
					}
				})
				.try_collect()
				.await?;
		}

		// Get the blob.
		let blob = match blobs.len() {
			0 => self.create_blob_inner_empty_leaf().await?,
			1 => blobs[0].clone(),
			_ => self.create_blob_inner_branch(blobs).await?,
		};

		Ok(blob)
	}

	async fn create_blob_inner_empty_leaf(&self) -> tg::Result<Blob> {
		let bytes = Bytes::default();
		let id = tg::leaf::Id::new(&bytes);
		Ok(Blob {
			bytes: None,
			count: 1,
			depth: 1,
			weight: 0,
			children: Vec::new(),
			data: None,
			size: 0,
			id: id.into(),
			length: 0,
			position: 0,
		})
	}

	async fn create_blob_inner_leaf(&self, chunk: &fastcdc::v2020::ChunkData) -> tg::Result<Blob> {
		let fastcdc::v2020::ChunkData {
			offset: position,
			length,
			data,
			..
		} = chunk;
		let length = length.to_u64().unwrap();
		let size = length;
		let id = tg::leaf::Id::new(data);
		let count = 1;
		let depth = 1;
		let weight = length;
		let output = Blob {
			bytes: None,
			children: Vec::new(),
			count,
			data: None,
			size,
			depth,
			id: id.into(),
			position: *position,
			length,
			weight,
		};
		Ok(output)
	}

	async fn create_blob_inner_branch(&self, children: Vec<Blob>) -> tg::Result<Blob> {
		let children_ = children
			.iter()
			.map(|child| tg::branch::data::Child {
				blob: child.id.clone(),
				length: child.length,
			})
			.collect();
		let data = tg::branch::Data {
			children: children_,
		};
		let bytes = data.serialize()?;
		let size = bytes.len().to_u64().unwrap();
		let id = tg::branch::Id::new(&bytes);
		let (count, depth, weight) =
			children
				.iter()
				.fold((1, 1, size), |(count, depth, weight), child| {
					(
						count + child.count,
						depth.max(child.depth),
						weight + child.weight,
					)
				});
		let position = children.first().unwrap().position;
		let length = children.iter().map(|child| child.length).sum();
		let output = Blob {
			bytes: Some(bytes),
			children,
			count,
			data: Some(data.into()),
			size,
			depth,
			id: id.into(),
			position,
			length,
			weight,
		};
		Ok(output)
	}

	async fn blob_create_messenger(
		&self,
		blob: &Blob,
		cache_reference: Option<tg::artifact::Id>,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			let children = blob
				.data
				.as_ref()
				.map(tg::blob::Data::children)
				.unwrap_or_default();
			let id = blob.id.clone().into();
			let size = blob.size;
			let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
				cache_reference: cache_reference.clone(),
				children,
				id,
				size,
				touched_at,
			});
			let message = serde_json::to_vec(&message)
				.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
			let _published = self
				.messenger
				.stream_publish("index".to_owned(), message.into())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
			stack.extend(&blob.children);
		}
		if let Some(id) = cache_reference {
			let message =
				crate::index::Message::PutCacheEntry(crate::index::PutCacheEntryMessage {
					id,
					touched_at,
				});
			let message = serde_json::to_vec(&message)
				.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
			let _published = self
				.messenger
				.stream_publish("index".to_owned(), message.into())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
		}
		Ok(())
	}

	async fn blob_create_store(
		&self,
		blob: &Blob,
		referenced_cache_entry: Option<tg::artifact::Id>,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut objects = Vec::new();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			let cache_reference =
				referenced_cache_entry
					.as_ref()
					.map(|artifact| crate::store::CacheReference {
						artifact: artifact.clone(),
						subpath: None,
						position: blob.position,
						length: blob.length,
					});
			objects.push((blob.id.clone().into(), blob.bytes.clone(), cache_reference));
			stack.extend(&blob.children);
		}
		let arg = crate::store::PutBatchArg {
			objects,
			touched_at,
		};
		self.store
			.put_batch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
	}

	pub(crate) async fn handle_create_blob_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let output = handle.create_blob(request.reader()).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
