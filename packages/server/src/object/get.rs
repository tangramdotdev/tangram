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
		// Attempt locally if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(output) = self
				.try_get_object_local(id)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the object locally"))?
		{
			return Ok(Some(output));
		}

		// Attempt remotely if requested.
		let remotes = self
			.remotes(arg.remotes)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(output) = self.try_get_object_remote(id, &remotes).await.map_err(
			|source| tg::error!(!source, %id, "failed to get the object from the remote"),
		)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub async fn try_get_object_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Attempt to get the bytes from the store.
		let object = self
			.store
			.try_get_object(id)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;

		// Get the bytes from the object.
		let mut bytes: Option<bytes::Bytes> =
			object.and_then(|o| o.bytes.map(|b| b.into_owned().into()));

		// If the bytes were not in the store, then attempt to read the bytes from the cache.
		if bytes.is_none()
			&& let Ok(id) = id.try_unwrap_blob_ref()
		{
			bytes = self.try_read_blob_from_cache(id).await?;
		}

		// If the bytes were not found, then return None.
		let Some(bytes) = bytes else {
			return Ok(None);
		};

		// Create the output.
		let output = tg::object::get::Output { bytes };

		Ok(Some(output))
	}

	pub(crate) fn try_get_object_sync(
		&self,
		id: &tg::object::Id,
		file: &mut Option<File>,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Attempt to get the bytes from the store.
		let object = self.store.try_get_object_sync(id)?;

		// Get the bytes from the object.
		let mut bytes: Option<bytes::Bytes> =
			object.and_then(|o| o.bytes.map(|b| b.into_owned().into()));

		// If the bytes were not in the store, then attempt to read the bytes from the cache.
		if bytes.is_none()
			&& let Ok(id) = id.try_unwrap_blob_ref()
		{
			bytes = self.try_read_blob_from_cache_sync(id, file)?;
		}

		// If the bytes were not found, then return None.
		let Some(bytes) = bytes else {
			return Ok(None);
		};

		// Create the output.
		let output = tg::object::get::Output { bytes };

		Ok(Some(output))
	}

	pub async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		let outputs = self
			.try_get_object_batch_local(ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects locally"))?;
		let remotes = self
			.remotes(None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let outputs = std::iter::zip(ids, outputs)
			.map(|(id, output)| async {
				if let Some(output) = output {
					return Ok(Some(output));
				}
				let output = self.try_get_object_remote(id, &remotes).await?;
				Ok::<_, tg::Error>(output)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	pub async fn try_get_object_batch_local(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		// Attempt to get the bytes from the store.
		let mut outputs = self
			.store
			.try_get_object_batch(ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get objects"))?
			.into_iter()
			.map(|option| {
				option
					.and_then(|o| o.bytes)
					.map(|bytes| tg::object::get::Output {
						bytes: bytes.into_owned().into(),
					})
			})
			.collect::<Vec<_>>();

		// If the bytes were not in the store, then attempt to read the bytes from the cache.
		for (id, output) in std::iter::zip(ids, outputs.iter_mut()) {
			if output.is_none()
				&& let Ok(id) = id.try_unwrap_blob_ref()
			{
				let bytes = self.try_read_blob_from_cache(id).await?;
				if let Some(bytes) = bytes {
					output.replace(tg::object::get::Output { bytes });
				}
			}
		}

		Ok(outputs)
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
		remotes: &[String],
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
				let arg = tg::object::get::Arg::default();
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
					local: None,
					remotes: None,
				};
				server.put_object(&id, arg).await?;
				Ok::<_, tg::Error>(())
			}
		});

		Ok(Some(output))
	}

	async fn try_read_blob_from_cache(&self, id: &tg::blob::Id) -> tg::Result<Option<Bytes>> {
		let object = self
			.store
			.try_get_object(&id.clone().into())
			.await
			.map_err(|error| tg::error!(!error, "failed to get the object"))?;
		let cache_pointer = object.and_then(|o| o.cache_pointer);
		let Some(cache_pointer) = cache_pointer else {
			return Ok(None);
		};

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

	fn try_read_blob_from_cache_sync(
		&self,
		id: &tg::blob::Id,
		file: &mut Option<File>,
	) -> tg::Result<Option<Bytes>> {
		// Get the cache pointer.
		let object = self.store.try_get_object_sync(&id.clone().into())?;
		let cache_pointer = object.and_then(|o| o.cache_pointer);
		let Some(cache_pointer) = cache_pointer else {
			return Ok(None);
		};

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
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the object id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let Some(output) = self.try_get_object_with_context(context, &id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().bytes(output.bytes).unwrap();
		Ok(response)
	}
}
