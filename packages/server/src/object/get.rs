use {
	crate::{Server, Session},
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
	tangram_archive::Archive as _,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_store::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
};

pub(crate) type Tasks = tangram_futures::task::Map<
	crate::object::get::TaskKey,
	tg::Result<Option<tg::object::get::Output>>,
	(),
	fnv::FnvBuildHasher,
>;

pub(crate) struct CheckoutFile {
	pub artifact: tg::artifact::Id,
	pub file: std::fs::File,
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TaskKey {
	pub availability: bool,
	pub id: tg::object::Id,
	pub location: tg::Location,
	pub metadata: bool,
	pub tokens: tg::authorization::Tokens,
}

impl Session {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_object_local(id, arg.metadata, arg.availability, arg.tokens.local())
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_get_object_regions(
					id,
					&local.regions,
					arg.metadata,
					arg.availability,
					&arg.tokens,
				)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the object from another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_get_object_remotes(
				id,
				&locations.remotes,
				arg.metadata,
				arg.availability,
				&arg.tokens,
			)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object from a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_object_local(
		&self,
		id: &tg::object::Id,
		metadata: bool,
		availability: bool,
		token: Option<&tg::authorization::Token>,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let resource = tg::Referent::with_node_and_token(id.clone(), token.cloned());
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let wait_for_subtree = metadata || availability;
		let Some(permissions) = self
			.authorize_object_read(resource, wait_for_subtree)
			.await?
		else {
			tracing::trace!(%id, principal = ?self.context.principal, "authorization denied");
			return Ok(None);
		};
		if !permissions.contains(node) {
			tracing::trace!(%id, principal = ?self.context.principal, "authorization denied");
			return Ok(None);
		}
		let Some(mut output) = self.server.try_get_object_local(id, metadata).await? else {
			return Ok(None);
		};
		let created_at = self.server.clock.unix_timestamp()?;
		let time_to_live = i64::try_from(self.server.config.object.grant_time_to_live.as_secs())
			.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
		let expires_at = created_at
			.checked_add(time_to_live)
			.ok_or_else(|| tg::error!("the grant expiration overflowed"))?;
		let resource = tg::Id::from(id.clone());
		if let Some(token) =
			self.create_token(resource, permissions.iter().collect(), expires_at)?
		{
			output.tokens.set_local(token);
		}
		if let Some(metadata) = output.metadata {
			output.metadata = Self::mask_object_metadata_with_permissions(metadata, permissions);
		}
		if availability && let Some(storage) = self.server.try_get_object_storage_local(id).await? {
			output.availability =
				Self::compute_object_availability_with_permissions(&storage, permissions);
		}
		Ok(Some(output))
	}

	pub(crate) async fn try_get_object_batch_local_or_regions(
		&self,
		objects: &[tg::Referent<tg::object::Id>],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		let outputs = self
			.try_get_object_batch_local(objects, metadata)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the objects locally"))?;
		let location: tg::location::Arg =
			tg::Location::Local(tg::location::Local::default()).into();
		let locations = self
			.locations(Some(&location))
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		let regions = locations.local.map_or_else(Vec::new, |local| local.regions);
		let outputs = std::iter::zip(objects, outputs)
			.map(|(object, output)| {
				let regions = regions.clone();
				async move {
					if let Some(output) = output {
						return Ok(Some(output));
					}

					self.try_get_object_regions(
						&object.node,
						&regions,
						metadata,
						false,
						&object.options.tokens,
					)
					.await
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		Ok(outputs)
	}

	pub(crate) async fn try_get_object_batch_local(
		&self,
		objects: &[tg::Referent<tg::object::Id>],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		// Get the objects.
		let ids = objects
			.iter()
			.map(|object| object.node.clone())
			.collect::<Vec<_>>();
		let outputs = self
			.server
			.try_get_object_batch_local(&ids, metadata)
			.await?;

		// Authorize the objects.
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let mut resources = Vec::new();
		let mut positions = Vec::new();
		for (position, (object, output)) in std::iter::zip(objects, &outputs).enumerate() {
			if output.is_none() {
				continue;
			}
			resources.push(object.clone());
			positions.push(position);
		}
		let authorizations = self
			.authorize_object_read_batch(resources, metadata)
			.await?;
		let mut permissions = vec![None; objects.len()];
		for (position, authorization) in std::iter::zip(positions, authorizations) {
			permissions[position] = authorization;
		}

		// Mask the outputs.
		let outputs = std::iter::zip(std::iter::zip(objects, outputs), permissions)
			.map(|((object, output), permissions)| {
				let mut output = output?;
				let Some(permissions) = permissions else {
					tracing::trace!(
						id = %object.node,
						principal = ?self.context.principal,
						"authorization denied"
					);

					return None;
				};
				if !permissions.contains(node) {
					tracing::trace!(
						id = %object.node,
						principal = ?self.context.principal,
						"authorization denied"
					);

					return None;
				}
				if let Some(metadata) = output.metadata.take() {
					output.metadata =
						Self::mask_object_metadata_with_permissions(metadata, permissions);
				}

				Some(output)
			})
			.collect();

		Ok(outputs)
	}

	async fn try_get_object_regions(
		&self,
		id: &tg::object::Id,
		regions: &[String],
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_object_region(id, region, metadata, availability, tokens))
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

		self.spawn_remote_object_put_task(id, &output);

		Ok(Some(output))
	}
	async fn try_get_object_region(
		&self,
		id: &tg::object::Id,
		region: &str,
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let Some(output) = self
			.try_get_object_location(id, location, metadata, availability, tokens)
			.await
			.map_err(
				|error| tg::error!(!error, %id, region = %region, "failed to get the object"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_get_object_remotes(
		&self,
		id: &tg::object::Id,
		remotes: &[crate::location::Remote],
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_object_remote(id, remote, metadata, availability, tokens))
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

		self.spawn_remote_object_put_task(id, &output);

		Ok(Some(output))
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
		remote: &crate::location::Remote,
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let Some(output) = self
			.try_get_object_location(id, location, metadata, availability, tokens)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					%id,
					remote = %remote.name,
					"failed to get the object"
				)
			})?
		else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_get_object_location(
		&self,
		id: &tg::object::Id,
		location: tg::Location,
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let key = TaskKey {
			availability,
			id: id.clone(),
			location,
			metadata,
			tokens: tokens.clone(),
		};
		self.try_get_object_from_location_task(key).await
	}

	async fn try_get_object_from_location_task(
		&self,
		key: TaskKey,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let task = self
			.server
			.object_get_tasks
			.get_or_spawn_detached(key.clone(), {
				let session = self.clone();
				move |_stop| async move { session.try_get_object_from_location_task_inner(key).await }
			});
		task.wait()
			.await
			.map_err(|error| tg::error!(!error, "the get object task panicked"))?
	}

	async fn try_get_object_from_location_task_inner(
		&self,
		key: TaskKey,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let TaskKey {
			availability,
			id,
			location,
			metadata,
			tokens,
		} = key;
		let source = location.clone();
		let tokens = tokens.for_location(&source);
		let (mut output, trusted) = match location {
			tg::Location::Local(local) => {
				let region = local
					.region
					.as_ref()
					.ok_or_else(|| tg::error!("expected the region to be set"))?;
				let client = self.get_region_session_for_process(region).await.map_err(
					|error| tg::error!(!error, region = %region, "failed to get the region client"),
				)?;
				let location = tg::Location::Local(tg::location::Local {
					region: Some(region.to_owned()),
				});
				let arg = tg::object::get::Arg {
					availability,
					location: Some(location.into()),
					metadata,
					tokens,
				};
				let output = client.try_get_object(&id, arg).await.map_err(
					|error| tg::error!(!error, %id, region = %region, "failed to get the object"),
				)?;
				(output, false)
			},
			tg::Location::Remote(remote) => {
				let client = self
					.get_remote_session_for_process(&remote.name)
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							remote = %remote.name,
							"failed to get the remote client"
						)
					})?;
				let arg = tg::object::get::Arg {
					availability,
					location: Some(remote.region.as_deref().map_or_else(
						|| tg::Location::Local(tg::location::Local::default()).into(),
						|region| {
							tg::Location::Local(tg::location::Local {
								region: Some(region.to_owned()),
							})
							.into()
						},
					)),
					metadata,
					tokens,
				};
				let trusted = client.trusted();
				let output = client.try_get_object(&id, arg).await.map_err(
					|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the object"),
				)?;
				(output, trusted)
			},
		};
		if !trusted && let Some(output) = &output {
			let actual = tg::object::Id::new(id.kind(), &output.bytes);
			if id != actual {
				return Err(tg::error!(
					expected = %id,
					actual = %actual,
					"invalid object id"
				));
			}
		}
		if let Some(output) = &mut output {
			self.update_tokens_and_location(&mut output.tokens, None, &source, trusted)?;
		}
		Ok(output)
	}

	fn spawn_remote_object_put_task(&self, id: &tg::object::Id, output: &tg::object::get::Output) {
		self.server
			.remote_object_put_tasks
			.spawn(|_| {
				let session = self.clone();
				let id = id.clone();
				let output = output.clone();
				async move {
					let arg = tg::object::put::Arg {
						bytes: output.bytes.clone(),
						children: Vec::new(),
						location: None,
						metadata: output.metadata.clone(),
					};
					let result = session.put_object(&id, arg).await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), "failed to put the remote object");
					}
				}
			})
			.detach();
	}
	pub(crate) async fn try_get_object_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Parse the object id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the object id"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the object.
		let Some(output) = self.try_get_object(&id, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::OCTET_STREAM)) => {
				let content_type = mime::APPLICATION_OCTET_STREAM;
				let body = BoxBody::with_bytes(output.bytes);
				(Some(content_type), body)
			},
			Some((mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let data = tg::object::Data::deserialize(id.kind(), output.bytes)
					.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;
				let body = serde_json::to_vec(&data)
					.map_err(|error| tg::error!(!error, "failed to serialize the object"))?;
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		if let Some(metadata) = &output.metadata {
			response = response
				.header_json(tg::object::get::METADATA_HEADER, metadata)
				.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
		}
		if let Some(availability) = &output.availability {
			response = response
				.header_json(tg::object::get::AVAILABILITY_HEADER, availability)
				.map_err(|error| tg::error!(!error, "failed to serialize the availability"))?;
		}
		if !output.tokens.is_empty() {
			response = response
				.header_json(tg::object::get::TOKENS_HEADER, &output.tokens)
				.map_err(|error| tg::error!(!error, "failed to serialize the tokens"))?;
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

impl Server {
	pub(crate) async fn try_get_object_local(
		&self,
		id: &tg::object::Id,
		metadata: bool,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let bytes_future = self.try_get_object_bytes_local(id);
		let metadata_future = async {
			if metadata {
				self.index
					.try_get_object(id)
					.await
					.ok()
					.flatten()
					.map(|object| object.metadata)
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
		let output = tg::object::get::Output {
			availability: None,
			bytes,
			metadata,
			tokens: tg::authorization::Tokens::default(),
		};

		Ok(Some(output))
	}

	pub(crate) fn try_get_object_sync(
		&self,
		id: &tg::object::Id,
		checkout_file: &mut Option<CheckoutFile>,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let arg = crate::store::object::get::Arg {
			id: id.clone(),
			put: None,
		};
		let output = self.store.try_get_object_sync(&arg)?;
		let object = output.object;
		let Some(object) = object else {
			return Ok(None);
		};
		let bytes = if let Some(bytes) = object.bytes {
			bytes.into_owned().into()
		} else if self.checkouts_enabled()
			&& let Some(checkout_pointer) = object.checkout_pointer
		{
			let Some(bytes) =
				self.try_read_checkout_pointer_sync(&checkout_pointer, checkout_file)?
			else {
				return Ok(None);
			};
			bytes
		} else {
			return Ok(None);
		};
		let output = tg::object::get::Output {
			availability: None,
			bytes,
			metadata: None,
			tokens: tg::authorization::Tokens::default(),
		};
		Ok(Some(output))
	}

	pub(crate) async fn try_get_object_batch_local(
		&self,
		ids: &[tg::object::Id],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::object::get::Output>>> {
		let bytes_future = self.try_get_object_bytes_batch_local(ids);
		let metadata_future = async {
			if metadata {
				self.index.try_get_objects(ids).await.ok().map(|objects| {
					objects
						.into_iter()
						.map(|object| object.map(|object| object.metadata))
						.collect()
				})
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
			.map(|(bytes, metadata)| {
				bytes.map(|bytes| tg::object::get::Output {
					availability: None,
					bytes,
					metadata,
					tokens: tg::authorization::Tokens::default(),
				})
			})
			.collect();

		Ok(outputs)
	}

	async fn try_get_object_bytes_batch_local(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Bytes>>> {
		let arg = crate::store::object::get::batch::Arg {
			ids: ids.to_owned(),
		};
		let output = self
			.store
			.try_get_object_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to get objects"))?;
		let output = ids
			.iter()
			.zip(output)
			.map(|(_id, output)| async move {
				let object = output.object;
				let Some(object) = object else {
					return Ok(None);
				};
				if let Some(bytes) = object.bytes {
					return Ok(Some(bytes.into_owned().into()));
				}
				if self.checkouts_enabled()
					&& let Some(checkout_pointer) = object.checkout_pointer
				{
					return self.try_read_checkout_pointer(&checkout_pointer).await;
				}
				Ok(None)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let output = ids
			.iter()
			.zip(output)
			.map(|(id, bytes)| async move {
				if bytes.is_some() {
					return Ok(bytes);
				}

				self.try_get_object_bytes_archive(id).await
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(output)
	}

	async fn try_get_object_bytes_local(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		let arg = crate::store::object::get::Arg {
			id: id.clone(),
			put: None,
		};
		let output = self
			.store
			.try_get_object(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		if let Some(object) = output.object {
			if let Some(bytes) = object.bytes {
				return Ok(Some(bytes.into_owned().into()));
			}
			if self.checkouts_enabled()
				&& let Some(checkout_pointer) = object.checkout_pointer
				&& let Some(bytes) = self.try_read_checkout_pointer(&checkout_pointer).await?
			{
				return Ok(Some(bytes));
			}
		}

		self.try_get_object_bytes_archive(id).await
	}

	async fn try_get_object_bytes_archive(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		let Some(archive) = &self.archive else {
			return Ok(None);
		};
		let object =
			self.index.try_get_object(id).await.map_err(
				|error| tg::error!(!error, %id, "failed to get the object from the index"),
			)?;
		if object.is_none() {
			return Ok(None);
		}
		let arg = tangram_archive::object::get::Arg { id: id.clone() };
		let output = archive.try_get_object(arg).await.map_err(
			|error| tg::error!(!error, %id, "failed to get the object from the archive"),
		)?;
		let Some(object) = output.object else {
			return Ok(None);
		};
		self.spawn_put_object_in_store_task(id.clone(), object.bytes.clone());

		Ok(Some(object.bytes))
	}

	fn spawn_put_object_in_store_task(&self, id: tg::object::Id, bytes: Bytes) {
		tokio::spawn({
			let server = self.clone();
			async move {
				let put = uuid::Uuid::now_v7().into_bytes();
				let object = crate::store::object::put::Arg {
					bytes: Some(bytes),
					checkout_pointer: None,
					id: id.clone(),
					length: None,
					put,
				};
				let result = if let Some(cache) = &server.config.object.cache {
					let partition = rand::random_range(0..cache.partition_total);
					let arg = crate::store::object::cache::put::object::Arg {
						cache: uuid::Uuid::now_v7().into_bytes(),
						object,
						partition,
					};
					server.store.put_object_cache_entry_with_object(arg).await
				} else {
					server.store.put_object(object).await
				};
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), %id, "failed to put an object in the store after reading it from the archive");
				}
			}
		});
	}

	async fn try_read_checkout_pointer(
		&self,
		checkout_pointer: &tangram_store::object::checkout::Pointer,
	) -> tg::Result<Option<Bytes>> {
		// Read the leaf from the file.
		let mut path = self
			.checkout_path()
			.join(checkout_pointer.artifact.to_string());
		if let Some(path_) = &checkout_pointer.path {
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
					"failed to open the entry in the checkouts directory"
				));
			},
		};

		// Seek.
		file.seek(std::io::SeekFrom::Start(checkout_pointer.position))
			.await
			.map_err(|error| tg::error!(!error, "failed to seek in the file"))?;

		// Read.
		let mut buffer = vec![0; 1 + checkout_pointer.length.to_usize().unwrap()];
		file.read_exact(&mut buffer[1..])
			.await
			.map_err(|error| tg::error!(!error, "failed to read the leaf from the file"))?;

		Ok(Some(buffer.into()))
	}

	fn try_read_checkout_pointer_sync(
		&self,
		checkout_pointer: &tangram_store::object::checkout::Pointer,
		checkout_file: &mut Option<CheckoutFile>,
	) -> tg::Result<Option<Bytes>> {
		// Replace the file if necessary.
		match checkout_file {
			Some(CheckoutFile { artifact, path, .. })
				if artifact == &checkout_pointer.artifact && path == &checkout_pointer.path => {},
			_ => {
				drop(checkout_file.take());
				let mut path = self
					.checkout_path()
					.join(checkout_pointer.artifact.to_string());
				if let Some(path_) = &checkout_pointer.path {
					path = path.join(path_);
				}
				let file_ = std::fs::File::open(&path).map_err(
					|error| tg::error!(!error, path = %path.display(), "failed to open the file"),
				)?;
				checkout_file.replace(CheckoutFile {
					artifact: checkout_pointer.artifact.clone(),
					file: file_,
					path: checkout_pointer.path.clone(),
				});
			},
		}

		// Seek.
		let file_handle = &mut checkout_file.as_mut().unwrap().file;
		file_handle
			.seek(std::io::SeekFrom::Start(checkout_pointer.position))
			.map_err(|error| tg::error!(!error, "failed to seek the checkout file"))?;

		// Read.
		let mut buffer = vec![0u8; 1 + checkout_pointer.length.to_usize().unwrap()];
		file_handle
			.read_exact(&mut buffer[1..])
			.map_err(|error| tg::error!(!error, "failed to read from the checkout file"))?;

		Ok(Some(buffer.into()))
	}
}
