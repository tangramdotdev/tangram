use {
	crate::{Server, temp::Temp},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, stream},
	itertools::Itertools,
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::PathBuf,
		pin::pin,
		sync::Arc,
	},
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
	tangram_store::prelude::*,
	tokio::io::{AsyncRead, AsyncWriteExt as _},
};

const MAX_BRANCH_CHILDREN: usize = 1_024;
const MIN_LEAF_SIZE: u32 = 4_096;
const AVG_LEAF_SIZE: u32 = 65_536;
const MAX_LEAF_SIZE: u32 = 131_072;

#[derive(Clone, Debug)]
pub struct Output {
	pub bytes: Option<Bytes>,
	pub children: Vec<Output>,
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
	pub async fn write(&self, reader: impl AsyncRead) -> tg::Result<tg::write::Output> {
		// Get the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Create the destination.
		let destination = if self.config.advanced.shared_directory {
			Destination::Temp(Temp::new(self))
		} else {
			Destination::Store { touched_at }
		};

		// Create the blob.
		let blob = self.write_inner(reader, Some(&destination)).await?;
		let blob = Arc::new(blob);

		// Rename the temp file to the cache directory if necessary.
		let cache_reference = if let Destination::Temp(temp) = destination {
			let data = tg::file::Data::Node(tg::file::data::Node {
				contents: Some(blob.id.clone()),
				dependencies: BTreeMap::new(),
				executable: false,
			});
			let id = tg::file::Id::new(&data.serialize()?);
			let path = self.cache_path().join(id.to_string());
			tokio::fs::rename(temp.path(), path)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to rename the file to the blobs directory")
				})?;
			Some((id.into(), None))
		} else {
			None
		};

		// Store.
		self.write_store(&blob, cache_reference.clone(), touched_at)
			.await?;

		// Publish index messages.
		self.write_index(&blob, cache_reference.clone(), touched_at)
			.await?;

		// Create the output.
		let output = tg::write::Output {
			blob: blob.id.clone(),
		};

		Ok(output)
	}

	pub(crate) async fn write_inner(
		&self,
		reader: impl AsyncRead,
		destination: Option<&Destination>,
	) -> tg::Result<Output> {
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
			let blob = self.write_inner_leaf(&chunk).await?;

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
						stream::once(self.write_inner_branch(chunk)).left_stream()
					} else {
						stream::iter(chunk.into_iter().map(Ok)).right_stream()
					}
				})
				.try_collect()
				.await?;
		}

		// Get the blob.
		let blob = match blobs.len() {
			0 => self.write_inner_empty_leaf().await?,
			1 => blobs[0].clone(),
			_ => self.write_inner_branch(blobs).await?,
		};

		Ok(blob)
	}

	async fn write_inner_empty_leaf(&self) -> tg::Result<Output> {
		let bytes = vec![0];
		let id = tg::blob::Id::new(&bytes);
		Ok(Output {
			bytes: None,
			count: 1,
			depth: 1,
			weight: 1,
			children: Vec::new(),
			data: None,
			size: 1,
			id,
			length: 0,
			position: 0,
		})
	}

	async fn write_inner_leaf(&self, chunk: &fastcdc::v2020::ChunkData) -> tg::Result<Output> {
		let fastcdc::v2020::ChunkData {
			offset: position,
			length,
			data,
			..
		} = chunk;
		let length = length.to_u64().unwrap();
		let size = 1 + length;
		let mut data_ = vec![0];
		data_.extend_from_slice(data);
		let id = tg::blob::Id::new(&data_);
		let count = 1;
		let depth = 1;
		let weight = size;
		let output = Output {
			bytes: None,
			children: Vec::new(),
			count,
			data: None,
			size,
			depth,
			id,
			position: *position,
			length,
			weight,
		};
		Ok(output)
	}

	async fn write_inner_branch(&self, children: Vec<Output>) -> tg::Result<Output> {
		let children_ = children
			.iter()
			.map(|child| tg::blob::data::Child {
				blob: child.id.clone(),
				length: child.length,
			})
			.collect();
		let data = tg::blob::Data::Branch(tg::blob::data::Branch {
			children: children_,
		});
		let bytes = data.serialize()?;
		let size = bytes.len().to_u64().unwrap();
		let id = tg::blob::Id::new(&bytes);
		let (count, depth, weight) = children.iter().unique_by(|blob| &blob.id).fold(
			(1, 1, size),
			|(count, depth, weight), child| {
				(
					count + child.count,
					depth.max(1 + child.depth),
					weight + child.weight,
				)
			},
		);
		let position = children.first().unwrap().position;
		let length = children.iter().map(|child| child.length).sum();
		let output = Output {
			bytes: Some(bytes),
			children,
			count,
			data: Some(data),
			size,
			depth,
			id,
			position,
			length,
			weight,
		};
		Ok(output)
	}

	async fn write_store(
		&self,
		blob: &Output,
		cache_reference: Option<(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
	) -> tg::Result<()> {
		let arg = Self::write_store_args(blob, cache_reference.as_ref(), touched_at);
		self.store
			.put_batch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
	}

	pub(crate) fn write_store_args(
		blob: &Output,
		cache_reference: Option<&(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
	) -> Vec<crate::store::PutArg> {
		let mut args = Vec::new();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			let cache_reference =
				cache_reference
					.as_ref()
					.map(|(artifact, path)| crate::store::CacheReference {
						artifact: artifact.clone(),
						path: path.clone(),
						position: blob.position,
						length: blob.length,
					});
			args.push(crate::store::PutArg {
				bytes: blob.bytes.clone(),
				cache_reference,
				id: blob.id.clone().into(),
				touched_at,
			});
			stack.extend(&blob.children);
		}
		args
	}

	async fn write_index(
		&self,
		blob: &Output,
		cache_reference: Option<(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
	) -> tg::Result<()> {
		let messages = Self::write_index_messages(blob, cache_reference, touched_at);
		let messages = messages
			.into_iter()
			.map(|message| message.serialize())
			.collect::<tg::Result<_>>()?;
		let _published = self
			.messenger
			.stream_batch_publish("index".to_owned(), messages)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the messages"))?;
		Ok(())
	}

	pub(crate) fn write_index_messages(
		blob: &Output,
		cache_reference: Option<(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
	) -> Vec<crate::index::Message> {
		// Collect the blobs in topological order.
		let mut blobs = Vec::new();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			blobs.push(blob);
			stack.extend(&blob.children);
		}

		// Create the messages.
		let mut messages = Vec::with_capacity(blobs.len() + 1);

		// Create put object messages in reverse topological order.
		for blob in blobs.into_iter().rev() {
			let cache_entry = cache_reference
				.as_ref()
				.map(|(artifact, _)| artifact.clone());
			let mut children = BTreeSet::new();
			if let Some(data) = &blob.data {
				data.children(&mut children);
			}
			let id = blob.id.clone().into();
			let metadata = tg::object::Metadata {
				count: Some(blob.count),
				depth: Some(blob.depth),
				weight: Some(blob.weight),
			};
			let size = blob.size;
			let message = crate::index::Message::PutObject(crate::index::message::PutObject {
				cache_entry,
				children,
				complete: true,
				id,
				metadata,
				size,
				touched_at,
			});
			messages.push(message);
		}

		// Create a cache entry message if necessary.
		if let Some((artifact, _)) = cache_reference {
			let message =
				crate::index::Message::PutCacheEntry(crate::index::message::PutCacheEntry {
					id: artifact,
					touched_at,
				});
			messages.push(message);
		}

		messages
	}

	pub(crate) async fn handle_write_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let output = handle.write(request.reader()).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
