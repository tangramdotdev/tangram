use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{
		FutureExt as _, future,
		stream::{FuturesOrdered, TryStreamExt as _},
	},
	num::ToPrimitive as _,
	std::{
		io::{Read as _, Seek as _},
		path::PathBuf,
	},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_store::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
};

pub struct File {
	pub artifact: tg::artifact::Id,
	pub path: Option<PathBuf>,
	#[expect(clippy::struct_field_names)]
	pub file: std::fs::File,
}

impl Server {
	pub async fn try_get_object_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		if Self::local(arg.local, arg.remotes.as_ref()) {
			let metadata = arg.metadata;
			let output = self
				.try_get_object_local(id, metadata)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the object locally"))?;
			if let Some(output) = output {
				return Ok(Some(output));
			}
		}

		let remotes = self
			.remotes(arg.local, arg.remotes)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(output) = self
			.try_get_object_remote(id, &remotes, arg.metadata)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to get the object from the remote"),
			)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_object_local(
		&self,
		id: &tg::object::Id,
		metadata: bool,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let bytes_future = self.try_get_object_bytes_local(id);
		let metadata_future = async {
			if metadata {
				self.try_get_object_metadata_local(id).await.ok().flatten()
			} else {
				None
			}
		};
		let (bytes, metadata) = future::join(bytes_future, metadata_future).await;
		let bytes = bytes?;

		// If the bytes were not found, then return None.
		let Some(bytes) = bytes else {
			return Ok(None);
		};

		// Create the output.
		let output = tg::object::get::Output { bytes, metadata };

		Ok(Some(output))
	}

	async fn try_get_object_bytes_local(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		let object = self
			.store
			.try_get_object(id)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let Some(object) = object else {
			return Ok(None);
		};
		if let Some(bytes) = object.bytes {
			return Ok(Some(bytes.into_owned().into()));
		}
		if let Some(cache_pointer) = object.cache_pointer {
			return self.try_read_cache_pointer(&cache_pointer).await;
		}
		Ok(None)
	}

	pub(crate) fn try_get_object_sync(
		&self,
		id: &tg::object::Id,
		file: &mut Option<File>,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let object = self.store.try_get_object_sync(id)?;
		let Some(object) = object else {
			return Ok(None);
		};
		let bytes = if let Some(bytes) = object.bytes {
			bytes.into_owned().into()
		} else if let Some(cache_pointer) = object.cache_pointer {
			let Some(bytes) = self.try_read_cache_pointer_sync(&cache_pointer, file)? else {
				return Ok(None);
			};
			bytes
		} else {
			return Ok(None);
		};
		let output = tg::object::get::Output {
			bytes,
			metadata: None,
		};
		Ok(Some(output))
	}

	#[expect(dead_code)]
	pub(crate) async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		let outputs = self
			.try_get_object_batch_local(ids, metadata)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects locally"))?;
		let remotes = self
			.remotes(None, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let outputs = std::iter::zip(ids, outputs)
			.map(|(id, output)| async {
				if let Some(output) = output {
					return Ok(Some(output));
				}
				let output = self.try_get_object_remote(id, &remotes, metadata).await?;
				Ok::<_, tg::Error>(output)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	pub(crate) async fn try_get_object_batch_local(
		&self,
		ids: &[tg::object::Id],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		let bytes_future = self.try_get_object_bytes_batch_local(ids);
		let metadata_future = async {
			if metadata {
				self.try_get_object_metadata_batch_local(ids).await.ok()
			} else {
				None
			}
			.unwrap_or_else(|| vec![None; ids.len()])
		};

		// Fetch bytes and metadata concurrently.
		let (bytes, metadata) = future::join(bytes_future, metadata_future).await;
		let bytes = bytes?;

		// Create the outputs.
		let outputs = std::iter::zip(bytes, metadata)
			.map(|(bytes, metadata)| bytes.map(|bytes| tg::object::get::Output { bytes, metadata }))
			.collect();

		Ok(outputs)
	}

	async fn try_get_object_bytes_batch_local(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Bytes>>> {
		let output = self
			.store
			.try_get_object_batch(ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get objects"))?;
		let output = output
			.into_iter()
			.map(|object| async {
				let Some(object) = object else {
					return Ok(None);
				};
				if let Some(bytes) = object.bytes {
					return Ok(Some(bytes.into_owned().into()));
				}
				if let Some(cache_pointer) = object.cache_pointer {
					return self.try_read_cache_pointer(&cache_pointer).await;
				}
				Ok(None)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(output)
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
		remotes: &[String],
		metadata: bool,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Attempt to get the object from the remotes.
		if remotes.is_empty() {
			return Ok(None);
		}
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				let arg = tg::object::get::Arg {
					metadata,
					..Default::default()
				};
				client
					.get_object(id, arg)
					.await
					.map_err(|source| tg::error!(!source, %id, %remote, "failed to get the object"))
			}
			.boxed()
		});
		let Ok((output, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		// Spawn a task to put the object.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let output = output.clone();
			async move {
				let arg = tg::object::put::Arg {
					bytes: output.bytes.clone(),
					metadata: output.metadata.clone(),
					local: None,
					remotes: None,
				};
				server.put_object(&id, arg).await?;
				Ok::<_, tg::Error>(())
			}
		});

		Ok(Some(output))
	}

	async fn try_read_cache_pointer(
		&self,
		cache_pointer: &tangram_store::CachePointer,
	) -> tg::Result<Option<Bytes>> {
		// Read the leaf from the file.
		let mut path = self.cache_path().join(cache_pointer.artifact.to_string());
		if let Some(path_) = &cache_pointer.path {
			path.push(path_);
		}
		let mut file = match tokio::fs::File::open(path).await {
			Ok(file) => file,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(error) => {
				return Err(tg::error!(
					!error,
					"failed to open the entry in the cache directory"
				));
			},
		};

		// Seek.
		file.seek(std::io::SeekFrom::Start(cache_pointer.position))
			.await
			.map_err(|source| tg::error!(!source, "failed to seek in the file"))?;

		// Read.
		let mut buffer = vec![0; 1 + cache_pointer.length.to_usize().unwrap()];
		file.read_exact(&mut buffer[1..])
			.await
			.map_err(|source| tg::error!(!source, "failed to read the leaf from the file"))?;

		Ok(Some(buffer.into()))
	}

	fn try_read_cache_pointer_sync(
		&self,
		cache_pointer: &tangram_store::CachePointer,
		file: &mut Option<File>,
	) -> tg::Result<Option<Bytes>> {
		// Replace the file if necessary.
		match file {
			Some(File { artifact, path, .. })
				if artifact == &cache_pointer.artifact && path == &cache_pointer.path => {},
			_ => {
				drop(file.take());
				let mut path = self.cache_path().join(cache_pointer.artifact.to_string());
				if let Some(path_) = &cache_pointer.path {
					path = path.join(path_);
				}
				let file_ = std::fs::File::open(&path).map_err(
					|source| tg::error!(!source, path = %path.display(), "failed to open the file"),
				)?;
				file.replace(File {
					artifact: cache_pointer.artifact.clone(),
					path: cache_pointer.path.clone(),
					file: file_,
				});
			},
		}

		// Seek.
		let file_handle = &mut file.as_mut().unwrap().file;
		file_handle
			.seek(std::io::SeekFrom::Start(cache_pointer.position))
			.map_err(|source| tg::error!(!source, "failed to seek the cache file"))?;

		// Read.
		let mut buffer = vec![0u8; 1 + cache_pointer.length.to_usize().unwrap()];
		file_handle
			.read_exact(&mut buffer[1..])
			.map_err(|source| tg::error!(!source, "failed to read from the cache file"))?;

		Ok(Some(buffer.into()))
	}

	pub(crate) async fn handle_get_object_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the object id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the object id"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the object.
		let Some(output) = self.try_get_object_with_context(context, &id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Validate the accept header.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::APPLICATION, mime::OCTET_STREAM)) => (),
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		}

		// Build the response.
		let mut response = http::Response::builder().header(
			http::header::CONTENT_TYPE,
			mime::APPLICATION_OCTET_STREAM.to_string(),
		);
		if let Some(metadata) = &output.metadata {
			response = response
				.header_json(tg::object::get::METADATA_HEADER, metadata)
				.map_err(|source| tg::error!(!source, "failed to serialize the metadata"))?;
		}
		let response = response.bytes(output.bytes).unwrap();

		Ok(response)
	}
}
