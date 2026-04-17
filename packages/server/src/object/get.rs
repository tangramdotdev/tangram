use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{
		StreamExt as _, future,
		stream::{FuturesOrdered, FuturesUnordered, TryStreamExt as _},
	},
	num::ToPrimitive as _,
	std::{
		io::{Read as _, Seek as _},
		path::PathBuf,
	},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_object_store::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
};

pub struct CacheFile {
	pub artifact: tg::artifact::Id,
	pub path: Option<PathBuf>,
	pub file: std::fs::File,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ObjectGetTaskKey {
	pub id: tg::object::Id,
	pub location: tg::location::Location,
	pub metadata: bool,
}

impl Server {
	pub async fn try_get_object_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let locations = self
			.locations_with_regions(arg.locations)
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_object_local(id, arg.metadata)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_get_object_from_regions(id, &local.regions, arg.metadata)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to get the object from another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_get_object_from_remotes(id, &locations.remotes, arg.metadata)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the object from a remote"))?
		{
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
			.object_store
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
		cache_file: &mut Option<CacheFile>,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let object = self.object_store.try_get_object_sync(id)?;
		let Some(object) = object else {
			return Ok(None);
		};
		let bytes = if let Some(bytes) = object.bytes {
			bytes.into_owned().into()
		} else if let Some(cache_pointer) = object.cache_pointer {
			let Some(bytes) = self.try_read_cache_pointer_sync(&cache_pointer, cache_file)? else {
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
		let locations = self
			.locations_with_regions(tg::location::Locations::default())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;
		let local_regions = locations.local.map_or_else(Vec::new, |local| local.regions);
		let remotes = locations.remotes;
		let outputs = std::iter::zip(ids, outputs)
			.map(|(id, output)| {
				let local_regions = local_regions.clone();
				let remotes = remotes.clone();
				async move {
					if let Some(output) = output {
						return Ok(Some(output));
					}

					if let Some(output) = self
						.try_get_object_from_regions(id, &local_regions, metadata)
						.await?
					{
						return Ok(Some(output));
					}

					let output = self
						.try_get_object_from_remotes(id, &remotes, metadata)
						.await?;
					Ok::<_, tg::Error>(output)
				}
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
			.object_store
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

	async fn try_get_object_from_regions(
		&self,
		id: &tg::object::Id,
		regions: &[String],
		metadata: bool,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_object_from_region(id, region, metadata))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};

		self.spawn_put_object_task(id, &output);

		Ok(Some(output))
	}

	async fn try_get_object_from_region(
		&self,
		id: &tg::object::Id,
		region: &str,
		metadata: bool,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let location = tg::location::Location::Local(tg::location::Local {
			regions: Some(vec![region.to_owned()]),
		});
		let Some(output) = self
			.try_get_object_from_location(id, location, metadata)
			.await
			.map_err(
				|source| tg::error!(!source, %id, region = %region, "failed to get the object"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_get_object_from_remotes(
		&self,
		id: &tg::object::Id,
		remotes: &[tg::location::Remote],
		metadata: bool,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_object_from_remote(id, remote, metadata))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};

		self.spawn_put_object_task(id, &output);

		Ok(Some(output))
	}

	async fn try_get_object_from_remote(
		&self,
		id: &tg::object::Id,
		remote: &tg::location::Remote,
		metadata: bool,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let location = tg::location::Location::Remote(remote.clone());
		let Some(output) = self
			.try_get_object_from_location(id, location, metadata)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					%id,
					remote = %remote.remote,
					"failed to get the object"
				)
			})?
		else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_get_object_from_location(
		&self,
		id: &tg::object::Id,
		location: tg::location::Location,
		metadata: bool,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let key = ObjectGetTaskKey {
			id: id.clone(),
			location,
			metadata,
		};
		self.try_get_object_from_location_task(key).await
	}

	async fn try_get_object_from_location_task(
		&self,
		key: ObjectGetTaskKey,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let task = self.object_get_tasks.get_or_spawn_detached(key.clone(), {
			let server = self.clone();
			move |_stop| async move { server.try_get_object_from_location_task_inner(key).await }
		});
		task.wait()
			.await
			.map_err(|source| tg::error!(!source, "the get object task panicked"))?
	}

	async fn try_get_object_from_location_task_inner(
		&self,
		key: ObjectGetTaskKey,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let ObjectGetTaskKey {
			id,
			location,
			metadata,
		} = key;
		match location {
			tg::location::Location::Local(local) => {
				let regions = local
					.regions
					.as_ref()
					.ok_or_else(|| tg::error!("expected the regions to be set"))?;
				let [region] = regions.as_slice() else {
					return Err(tg::error!("expected exactly one region"));
				};
				let client = self.get_region_client(region.clone()).await.map_err(
					|source| tg::error!(!source, region = %region, "failed to get the region client"),
				)?;
				let arg = tg::object::get::Arg {
					locations: tg::location::Locations {
						local: Some(tg::Either::Right(tg::location::Local {
							regions: Some(vec![region.clone()]),
						})),
						remotes: Some(tg::Either::Left(false)),
					},
					metadata,
				};
				client.try_get_object(&id, arg).await.map_err(
					|source| tg::error!(!source, %id, region = %region, "failed to get the object"),
				)
			},
			tg::location::Location::Remote(remote) => {
				let client = self
					.get_remote_client(remote.remote.clone())
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							remote = %remote.remote,
							"failed to get the remote client"
						)
					})?;
				let arg = tg::object::get::Arg {
					locations: tg::location::Locations {
						local: match &remote.regions {
							Some(regions) => Some(tg::Either::Right(tg::location::Local {
								regions: Some(regions.clone()),
							})),
							None => Some(tg::Either::Left(true)),
						},
						remotes: Some(tg::Either::Left(false)),
					},
					metadata,
				};
				client.try_get_object(&id, arg).await.map_err(
					|source| tg::error!(!source, %id, remote = %remote.remote, "failed to get the object"),
				)
			},
		}
	}

	fn spawn_put_object_task(&self, id: &tg::object::Id, output: &tg::object::get::Output) {
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let output = output.clone();
			async move {
				let arg = tg::object::put::Arg {
					bytes: output.bytes.clone(),
					location: None,
					metadata: output.metadata.clone(),
				};
				server.put_object(&id, arg).await?;
				Ok::<_, tg::Error>(())
			}
		});
	}

	async fn try_read_cache_pointer(
		&self,
		cache_pointer: &tangram_object_store::CachePointer,
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
		cache_pointer: &tangram_object_store::CachePointer,
		cache_file: &mut Option<CacheFile>,
	) -> tg::Result<Option<Bytes>> {
		// Replace the file if necessary.
		match cache_file {
			Some(CacheFile { artifact, path, .. })
				if artifact == &cache_pointer.artifact && path == &cache_pointer.path => {},
			_ => {
				drop(cache_file.take());
				let mut path = self.cache_path().join(cache_pointer.artifact.to_string());
				if let Some(path_) = &cache_pointer.path {
					path = path.join(path_);
				}
				let file_ = std::fs::File::open(&path).map_err(
					|source| tg::error!(!source, path = %path.display(), "failed to open the file"),
				)?;
				cache_file.replace(CacheFile {
					artifact: cache_pointer.artifact.clone(),
					path: cache_pointer.path.clone(),
					file: file_,
				});
			},
		}

		// Seek.
		let file_handle = &mut cache_file.as_mut().unwrap().file;
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
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
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
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Validate the accept header.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::OCTET_STREAM)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
		let response = response.bytes(output.bytes).unwrap().boxed_body();

		Ok(response)
	}
}
