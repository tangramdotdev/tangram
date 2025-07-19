use crate::Server;
use bytes::Bytes;
use futures::{
	FutureExt as _, future,
	stream::{FuturesUnordered, TryStreamExt as _},
};
use num::ToPrimitive as _;
use std::{
	io::{Read as _, Seek as _},
	path::PathBuf,
};
use tangram_client::{self as tg, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

impl Server {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		if let Some(output) = self.try_get_object_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_object_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	pub async fn try_get_object_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Attempt to get the bytes from the store.
		let mut bytes = self.store.try_get(id).await?;

		// If the bytes were not in the store, then attempt to read the bytes from the cache.
		if bytes.is_none() {
			if let Ok(id) = id.try_unwrap_blob_ref() {
				bytes = self.try_read_blob_from_cache(id).await?;
			}
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
		file: &mut Option<(tg::artifact::Id, Option<PathBuf>, std::fs::File)>,
	) -> tg::Result<Option<tg::object::get::Output>> {
		#[allow(clippy::match_wildcard_for_single_variants)]
		let mut bytes = match &self.store {
			crate::store::Store::Lmdb(lmdb) => lmdb.try_get_sync(id)?,
			crate::store::Store::Memory(memory) => memory.try_get(id),
			_ => {
				return Err(tg::error!("invalid store"));
			},
		};

		// If the bytes were not in the store, then attempt to read the bytes from the cache.
		if bytes.is_none() {
			if let Ok(id) = id.try_unwrap_blob_ref() {
				bytes = self.try_read_blob_from_cache_sync(id, file)?;
			}
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
		let outputs = self.try_get_object_batch_local(ids).await?;
		let outputs = std::iter::zip(ids, outputs)
			.map(|(id, output)| async move {
				if let Some(output) = output {
					return Ok(Some(output));
				}
				let output = self.try_get_object_remote(id).await?;
				Ok::<_, tg::Error>(output)
			})
			.collect::<FuturesUnordered<_>>()
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
			.try_get_batch(ids)
			.await?
			.into_iter()
			.map(|option| option.map(|bytes| tg::object::get::Output { bytes }))
			.collect::<Vec<_>>();

		// If the bytes were not in the store, then attempt to read the bytes from the cache.
		for (id, output) in std::iter::zip(ids, outputs.iter_mut()) {
			if output.is_none() {
				if let Ok(id) = id.try_unwrap_blob_ref() {
					let bytes = self.try_read_blob_from_cache(id).await?;
					if let Some(bytes) = bytes {
						output.replace(tg::object::get::Output { bytes });
					}
				}
			}
		}

		Ok(outputs)
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Attempt to get the object from the remotes.
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| async move { client.get_object(id).await }.boxed())
			.collect::<Vec<_>>();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((output, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		// Validate the server didn't give us back garbage.
		if &tg::object::Id::new(id.kind(), &output.bytes) != id {
			return Err(tg::error!(?bytes = output.bytes, "server returned an invalid object"));
		}

		// Spawn a task to put the object.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let output = output.clone();
			async move {
				let arg = tg::object::put::Arg {
					bytes: output.bytes.clone(),
				};
				server.put_object(&id, arg).await?;
				Ok::<_, tg::Error>(())
			}
		});

		Ok(Some(output))
	}

	async fn try_read_blob_from_cache(&self, id: &tg::blob::Id) -> tg::Result<Option<Bytes>> {
		let cache_reference = self
			.store
			.try_get_cache_reference(&id.clone().into())
			.await?;
		let Some(cache_reference) = cache_reference else {
			return Ok(None);
		};

		// Read the leaf from the file.
		let mut path = self.cache_path().join(cache_reference.artifact.to_string());
		if let Some(path_) = &cache_reference.path {
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
		file.seek(std::io::SeekFrom::Start(cache_reference.position))
			.await
			.map_err(|source| tg::error!(!source, "failed to seek in the file"))?;

		// Read.
		let mut buffer = vec![0; 1 + cache_reference.length.to_usize().unwrap()];
		file.read_exact(&mut buffer[1..])
			.await
			.map_err(|source| tg::error!(!source, "failed to read the leaf from the file"))?;

		Ok(Some(buffer.into()))
	}

	fn try_read_blob_from_cache_sync(
		&self,
		id: &tg::blob::Id,
		file: &mut Option<(tg::artifact::Id, Option<PathBuf>, std::fs::File)>,
	) -> tg::Result<Option<Bytes>> {
		// Get the cache reference.
		#[allow(clippy::match_wildcard_for_single_variants)]
		let cache_reference = match &self.store {
			crate::store::Store::Lmdb(lmdb) => {
				lmdb.try_get_cache_reference_sync(&id.clone().into())?
			},
			crate::store::Store::Memory(memory) => {
				memory.try_get_cache_reference(&id.clone().into())
			},
			_ => {
				return Err(tg::error!("invalid store"));
			},
		};
		let Some(cache_reference) = cache_reference else {
			return Ok(None);
		};

		// Replace the file if necessary.
		match file {
			Some((artifact, path_, _))
				if artifact == &cache_reference.artifact && path_ == &cache_reference.path => {},
			_ => {
				drop(file.take());
				let mut path = self.cache_path().join(cache_reference.artifact.to_string());
				if let Some(path_) = &cache_reference.path {
					path = path.join(path_);
				}
				let file_ = std::fs::File::open(&path).map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
				)?;
				file.replace((
					cache_reference.artifact.clone(),
					cache_reference.path.clone(),
					file_,
				));
			},
		}

		// Seek.
		let (_, _, file) = file.as_mut().unwrap();
		file.seek(std::io::SeekFrom::Start(cache_reference.position))
			.map_err(|source| tg::error!(!source, "failed to seek the cache file"))?;

		// Read.
		let mut buffer = vec![0u8; 1 + cache_reference.length.to_usize().unwrap()];
		file.read_exact(&mut buffer[1..])
			.map_err(|source| tg::error!(!source, "failed to read from the cache file"))?;

		Ok(Some(buffer.into()))
	}

	pub(crate) async fn handle_get_object_request<H>(
		handle: &H,
		_request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_object(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().bytes(output.bytes).unwrap();
		Ok(response)
	}
}
