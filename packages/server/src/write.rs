use {
	crate::{Context, Server, temp::Temp},
	bytes::Bytes,
	futures::TryStreamExt as _,
	itertools::Itertools,
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		io::{Read, Write as _},
		path::PathBuf,
		pin::pin,
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
	tangram_store::prelude::*,
	tokio::io::{AsyncRead, AsyncWriteExt as _},
};

#[derive(Clone, Debug)]
pub struct Output {
	pub bytes: Option<Bytes>,
	pub children: Vec<Output>,
	pub data: Option<tg::blob::Data>,
	pub id: tg::blob::Id,
	pub length: u64,
	pub metadata: tg::object::Metadata,
	pub position: u64,
}

pub enum Destination {
	Temp(Temp),
	Store { touched_at: i64 },
}

impl Server {
	pub(crate) async fn write_with_context(
		&self,
		_context: &Context,
		reader: impl AsyncRead,
	) -> tg::Result<tg::write::Output> {
		// Get the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Create the destination.
		let destination = if self.config.advanced.single_directory {
			Destination::Temp(Temp::new(self))
		} else {
			Destination::Store { touched_at }
		};

		// Create the blob.
		let blob = self
			.write_inner(reader, Some(&destination))
			.await
			.map_err(|source| tg::error!(!source, "failed to write the blob"))?;
		let blob = Arc::new(blob);

		// Rename the temp file to the cache directory if necessary.
		let cache_pointer = if let Destination::Temp(temp) = destination {
			let data = tg::file::Data::Node(tg::file::data::Node {
				contents: Some(blob.id.clone()),
				dependencies: BTreeMap::new(),
				executable: false,
				module: None,
			});
			let id = tg::file::Id::new(&data.serialize()?);
			let path = self.cache_path().join(id.to_string());
			match tangram_util::fs::rename_noreplace(temp.path(), path).await {
				Ok(()) => (),
				Err(error)
					if matches!(
						error.kind(),
						std::io::ErrorKind::AlreadyExists
							| std::io::ErrorKind::IsADirectory
							| std::io::ErrorKind::PermissionDenied
					) => {},
				Err(source) => {
					return Err(tg::error!(
						!source,
						"failed to rename the file to the blobs directory"
					));
				},
			}
			Some((id.into(), None))
		} else {
			None
		};

		// Store.
		self.write_store(&blob, cache_pointer.clone(), touched_at)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the blob"))?;

		// Publish index messages.
		self.write_index(&blob, cache_pointer.clone(), touched_at)
			.await
			.map_err(|source| tg::error!(!source, "failed to index the blob"))?;

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
			self.config.write.min_leaf_size,
			self.config.write.avg_leaf_size,
			self.config.write.max_leaf_size,
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
			let blob = Self::write_inner_leaf(&chunk);

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
					let mut bytes = vec![0];
					bytes.extend_from_slice(&chunk.data);
					let arg = crate::store::PutObjectArg {
						bytes: Some(bytes.into()),
						cache_pointer: None,
						id: blob.id.clone().into(),
						touched_at: *touched_at,
					};
					self.store
						.put_object(arg)
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
		let max_branch_children = self.config.write.max_branch_children;
		while blobs.len() > max_branch_children {
			blobs = blobs
				.chunks(max_branch_children)
				.flat_map(|chunk| {
					if chunk.len() == max_branch_children {
						vec![Self::write_inner_branch(chunk.to_vec())]
					} else {
						chunk.iter().cloned().map(Ok).collect()
					}
				})
				.collect::<tg::Result<Vec<_>>>()?;
		}

		// Get the blob.
		let blob = match blobs.len() {
			0 => Self::write_inner_empty_leaf(),
			1 => blobs[0].clone(),
			_ => Self::write_inner_branch(blobs)?,
		};

		Ok(blob)
	}

	pub(crate) fn write_inner_sync(
		&self,
		mut reader: impl Read,
		destination: Option<&Destination>,
	) -> tg::Result<Output> {
		// Create the chunker.
		let config = &self.config.write;
		let mut chunker = fastcdc::v2020::StreamCDC::new(
			&mut reader,
			config.min_leaf_size,
			config.avg_leaf_size,
			config.max_leaf_size,
		);

		// Open the destination file if necessary.
		let mut file = if let Some(Destination::Temp(temp)) = destination {
			Some(
				std::fs::File::create_new(temp.path())
					.map_err(|source| tg::error!(!source, "failed to create the file"))?,
			)
		} else {
			None
		};

		// Create the leaves and write or store them if necessary.
		let mut blobs = Vec::new();
		for chunk in &mut chunker {
			let chunk =
				chunk.map_err(|source| tg::error!(!source, "failed to read from the reader"))?;

			// Create the leaf.
			let blob = Self::write_inner_leaf(&chunk);

			// Store the leaf if necessary.
			match destination {
				None => (),
				Some(Destination::Temp(_)) => {
					file.as_mut()
						.unwrap()
						.write_all(&chunk.data)
						.map_err(|source| tg::error!(!source, "failed to write to the file"))?;
				},
				Some(Destination::Store { touched_at }) => {
					let mut bytes = vec![0];
					bytes.extend_from_slice(&chunk.data);
					let arg = crate::store::PutObjectArg {
						bytes: Some(bytes.into()),
						cache_pointer: None,
						id: blob.id.clone().into(),
						touched_at: *touched_at,
					};
					self.store
						.put_object_sync(arg)
						.map_err(|source| tg::error!(!source, "failed to store the leaf"))?;
				},
			}

			blobs.push(blob);
		}

		// Flush and close the file if necessary.
		if let Some(mut file) = file {
			file.flush()
				.map_err(|source| tg::error!(!source, "failed to flush the file"))?;
			drop(file);
		}

		// Create the tree.
		let max_branch_children = config.max_branch_children;
		while blobs.len() > max_branch_children {
			blobs = blobs
				.chunks(max_branch_children)
				.flat_map(|chunk| {
					if chunk.len() == max_branch_children {
						vec![Self::write_inner_branch(chunk.to_vec())]
					} else {
						chunk.iter().cloned().map(Ok).collect()
					}
				})
				.collect::<tg::Result<Vec<_>>>()?;
		}

		// Get the blob.
		let blob = match blobs.len() {
			0 => Self::write_inner_empty_leaf(),
			1 => blobs[0].clone(),
			_ => Self::write_inner_branch(blobs)?,
		};

		Ok(blob)
	}

	fn write_inner_empty_leaf() -> Output {
		let bytes = vec![0];
		let id = tg::blob::Id::new(&bytes);
		Output {
			bytes: None,
			children: Vec::new(),
			data: None,
			id,
			length: 0,
			metadata: tg::object::Metadata {
				node: tg::object::metadata::Node {
					size: 1,
					solvable: false,
					solved: true,
				},
				subtree: tg::object::metadata::Subtree {
					count: Some(1),
					depth: Some(1),
					size: Some(1),
					solvable: Some(false),
					solved: Some(true),
				},
			},
			position: 0,
		}
	}

	fn write_inner_leaf(chunk: &fastcdc::v2020::ChunkData) -> Output {
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
		Output {
			bytes: None,
			children: Vec::new(),
			data: None,
			id,
			length,
			metadata: tg::object::Metadata {
				node: tg::object::metadata::Node {
					size,
					solvable: false,
					solved: true,
				},
				subtree: tg::object::metadata::Subtree {
					count: Some(1),
					depth: Some(1),
					size: Some(size),
					solvable: Some(false),
					solved: Some(true),
				},
			},
			position: *position,
		}
	}

	fn write_inner_branch(children: Vec<Output>) -> tg::Result<Output> {
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
		let metadata = children.iter().unique_by(|blob| &blob.id).fold(
			tg::object::Metadata {
				node: tg::object::metadata::Node {
					size,
					solvable: false,
					solved: true,
				},
				subtree: tg::object::metadata::Subtree {
					count: Some(1),
					depth: Some(1),
					size: Some(size),
					solvable: Some(false),
					solved: Some(true),
				},
			},
			|mut metadata, child| {
				let child_subtree = &child.metadata.subtree;
				metadata.subtree.count = metadata
					.subtree
					.count
					.zip(child_subtree.count)
					.map(|(a, b)| a + b);
				metadata.subtree.depth = metadata
					.subtree
					.depth
					.zip(child_subtree.depth)
					.map(|(a, b)| a.max(1 + b));
				metadata.subtree.size = metadata
					.subtree
					.size
					.zip(child_subtree.size)
					.map(|(a, b)| a + b);
				metadata
			},
		);
		let position = children.first().unwrap().position;
		let length = children.iter().map(|child| child.length).sum();
		let output = Output {
			bytes: Some(bytes),
			children,
			data: Some(data),
			id,
			length,
			metadata,
			position,
		};
		Ok(output)
	}

	async fn write_store(
		&self,
		blob: &Output,
		cache_pointer: Option<(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
	) -> tg::Result<()> {
		let arg = Self::write_store_args(blob, cache_pointer.as_ref(), touched_at);
		self.store
			.put_object_batch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
	}

	pub(crate) fn write_store_args(
		blob: &Output,
		cache_pointer: Option<&(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
	) -> Vec<crate::store::PutObjectArg> {
		let mut args = Vec::new();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			let cache_pointer =
				cache_pointer
					.as_ref()
					.map(|(artifact, path)| crate::store::CachePointer {
						artifact: artifact.clone(),
						path: path.clone(),
						position: blob.position,
						length: blob.length,
					});
			args.push(crate::store::PutObjectArg {
				bytes: blob.bytes.clone(),
				cache_pointer,
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
		cache_pointer: Option<(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
	) -> tg::Result<()> {
		let messages = Self::write_index_messages(blob, cache_pointer, touched_at);
		let message = crate::index::message::Messages(messages);
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the messages"))?;
		Ok(())
	}

	pub(crate) fn write_index_messages(
		blob: &Output,
		cache_pointer: Option<(tg::artifact::Id, Option<PathBuf>)>,
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
			let cache_entry = cache_pointer.as_ref().map(|(artifact, _)| artifact.clone());
			let mut children = BTreeSet::new();
			if let Some(data) = &blob.data {
				data.children(&mut children);
			}
			let id = blob.id.clone().into();
			let message = crate::index::Message::PutObject(crate::index::message::PutObject {
				cache_entry,
				children,
				id,
				metadata: blob.metadata.clone(),
				stored: crate::object::stored::Output { subtree: true },
				touched_at,
			});
			messages.push(message);
		}

		// Create a cache entry message if necessary.
		if let Some((artifact, _)) = cache_pointer {
			let message =
				crate::index::Message::PutCacheEntry(crate::index::message::PutCacheEntry {
					id: artifact,
					touched_at,
				});
			messages.push(message);
		}

		messages
	}

	pub(crate) async fn handle_write_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let output = self
			.write_with_context(context, request.reader())
			.await
			.map_err(|source| tg::error!(!source, "failed to write"))?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
