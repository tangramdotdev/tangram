use {
	crate::{Server, Session},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream, FuturesOrdered, FuturesUnordered},
	},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

impl Session {
	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_process_local(id, arg.metadata, arg.availability, arg.tokens.local())
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_get_process_regions(
					id,
					&local.regions,
					arg.metadata,
					arg.availability,
					&arg.tokens,
				)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the process from another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_get_process_remotes(
				id,
				&locations.remotes,
				arg.metadata,
				arg.availability,
				&arg.tokens,
			)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process from a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_batch_local_or_regions(
		&self,
		processes: &[tg::Referent<tg::process::Id>],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let location: tg::location::Arg =
			tg::Location::Local(tg::location::Local::default()).into();
		let outputs = processes
			.iter()
			.map(|process| {
				let arg = tg::process::get::Arg {
					availability: false,
					location: Some(location.clone()),
					metadata,
					tokens: process.options.tokens.clone(),
				};
				self.try_get_process(&process.node, arg)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		Ok(outputs)
	}

	pub(crate) async fn try_get_process_local(
		&self,
		id: &tg::process::Id,
		metadata: bool,
		availability: bool,
		token: Option<&tg::authorization::Token>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let resource = tg::Referent::with_node_and_token(id.clone(), token.cloned());
		let permissions = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::all(),
		);
		let authorize_future = async { self.authorize(resource, permissions).await }.boxed();
		let get_future = self.try_get_process_local_inner(id, metadata).boxed();
		let (permissions, output) = future::try_join(authorize_future, get_future).await?;
		let Some(permissions) = permissions else {
			return Ok(None);
		};
		let node = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		if !permissions.contains(node) {
			return Ok(None);
		}
		let Some(mut output) = output else {
			return Ok(None);
		};
		let created_at = self.server.clock.unix_timestamp()?;
		let time_to_live =
			i64::try_from(self.server.config.process.grant_time_to_live.as_secs())
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
		if let Some(metadata) = output.metadata.take() {
			output.metadata = self
				.mask_process_metadata(id, metadata, token)
				.boxed()
				.await?;
		}
		if availability && let Some(storage) = self.server.try_get_process_storage_local(id).await?
		{
			output.availability = self
				.compute_process_availability(id, storage, token)
				.await?;
		}
		Ok(Some(output))
	}

	pub(crate) async fn get_process_local(
		&self,
		id: &tg::process::Id,
		metadata: bool,
	) -> tg::Result<tg::process::get::Output> {
		let output = self
			.try_get_process_local_inner(id, metadata)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;

		Ok(output)
	}

	pub(crate) async fn set_process_children_from_index(
		&self,
		id: &tg::process::Id,
		children_set: bool,
		data: &mut tg::process::Data,
	) -> tg::Result<()> {
		if !children_set || data.children.is_some() {
			return Ok(());
		}
		let mut children = Vec::new();
		let mut position = 0;
		let length = 256;
		loop {
			let output = self
				.server
				.index
				.try_get_process_children(id, std::io::SeekFrom::Start(position), length)
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
			let output_length = u64::try_from(output.len()).unwrap();
			children.extend(output);
			if output_length < length {
				break;
			}
			position += output_length;
		}
		data.children = Some(children);

		Ok(())
	}

	pub(crate) async fn try_get_process_local_inner(
		&self,
		id: &tg::process::Id,
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		// Subscribe before reading to avoid missing a status change between the read and subscription.
		let mut wakeups = self
			.create_process_status_wakeup_stream(id, None, None)
			.await?;
		self.try_get_process_local_inner_with_wakeups(id, metadata, &mut wakeups)
			.await
	}

	pub(super) async fn try_get_process_local_inner_with_wakeups(
		&self,
		id: &tg::process::Id,
		metadata: bool,
		wakeups: &mut BoxStream<'static, ()>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		loop {
			tokio::select! {
				output = self.try_get_process_local_inner_attempt(id, metadata) => {
					return output;
				},
				wakeup = wakeups.next() => {
					if wakeup.is_none() {
						return Err(tg::error!("the process status wakeup stream ended"));
					}
				},
			}
		}
	}

	pub(super) async fn try_get_process_local_inner_attempt(
		&self,
		id: &tg::process::Id,
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		if let Some(data) = self.server.runner.state().try_get_process(id)
			&& !data.status.is_finished()
		{
			let metadata = if metadata {
				self.try_get_process_from_index(id)
					.await?
					.map(|process| process.metadata)
			} else {
				None
			};
			let output = self.create_process_get_output(id, data, metadata);
			return Ok(Some(output));
		}

		let index_future = self.try_get_process_from_index(id).boxed();
		let control_future = self.get_process_from_control(id).boxed();
		let output = match future::select(index_future, control_future).await {
			future::Either::Left((indexed, control_future)) => {
				let Some(indexed) = indexed? else {
					return Ok(None);
				};
				if indexed
					.data
					.as_ref()
					.is_some_and(|data| data.status.is_finished())
				{
					let data = indexed.data.unwrap();
					self.create_process_get_output(id, data, metadata.then_some(indexed.metadata))
				} else {
					// Give the runner a short opportunity to provide fresher data before falling back to the index.
					let Ok(Ok(data)) =
						tokio::time::timeout(std::time::Duration::from_secs(1), control_future)
							.await
					else {
						let data = indexed
							.data
							.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
						let output = self.create_process_get_output(
							id,
							data,
							metadata.then_some(indexed.metadata),
						);
						return Ok(Some(output));
					};
					if data.status.is_finished() {
						let Some(indexed) = self.try_get_process_from_index(id).await? else {
							return Ok(None);
						};
						let data = indexed
							.data
							.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
						self.create_process_get_output(
							id,
							data,
							metadata.then_some(indexed.metadata),
						)
					} else {
						self.create_process_get_output(
							id,
							data,
							metadata.then_some(indexed.metadata),
						)
					}
				}
			},
			future::Either::Right((data, index_future)) => {
				let Ok(data) = data else {
					let Some(indexed) = index_future.await? else {
						return Ok(None);
					};
					let data = indexed
						.data
						.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
					let output = self.create_process_get_output(
						id,
						data,
						metadata.then_some(indexed.metadata),
					);
					return Ok(Some(output));
				};
				if data.status.is_finished() {
					let Some(indexed) = self.try_get_process_from_index(id).await? else {
						return Ok(None);
					};
					let data = indexed
						.data
						.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
					self.create_process_get_output(id, data, metadata.then_some(indexed.metadata))
				} else {
					let indexed = if metadata { index_future.await? } else { None };
					let metadata = indexed.map(|process| process.metadata);
					self.create_process_get_output(id, data, metadata)
				}
			},
		};

		Ok(Some(output))
	}

	pub(crate) async fn get_process_from_index(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tangram_index::process::Process> {
		self.try_get_process_from_index(id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process in the index"))
	}

	pub(crate) async fn try_get_process_from_index(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::Process>> {
		if let Some(process) = self.server.index.try_get_process(id).await? {
			return Ok(Some(process));
		}
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		self.server.index.try_get_process(id).await
	}

	pub(crate) async fn get_process_from_control(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tg::process::Data> {
		let request = tg::process::control::ServerRequestArg::Get(
			tg::process::control::GetServerRequestArg {},
		);
		let retry = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		};
		let options = crate::control::Options {
			retry,
			timeout: std::time::Duration::from_secs(10),
		};
		let response = self
			.send_process_control_request(id, request, options)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the get process control request"),
			)?
			.map_err(|error| tg::error!(!error, %id, "the get process control request failed"))?;
		let response = response
			.try_unwrap_get()
			.map_err(|_| tg::error!("expected a get response"))?;
		let output = response.data;
		Ok(output)
	}

	fn create_process_get_output(
		&self,
		id: &tg::process::Id,
		data: tg::process::Data,
		metadata: Option<tg::process::Metadata>,
	) -> tg::process::get::Output {
		let data = data.without_location_and_tokens();
		let location = self.server.config().region.clone().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|region| {
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				})
			},
		);
		tg::process::get::Output {
			availability: None,
			data,
			id: id.clone(),
			location: Some(location),
			metadata,
			tokens: tg::authorization::Tokens::default(),
		}
	}

	async fn try_get_process_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_region(id, region, metadata, availability, tokens))
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
		Ok(Some(output))
	}

	async fn try_get_process_region(
		&self,
		id: &tg::process::Id,
		region: &str,
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::get::Arg {
			availability,
			location: Some(location.clone().into()),
			metadata,
			tokens: tokens.for_location(&location),
		};
		let Some(mut output) = client.try_get_process(id, arg).await.map_err(
			|error| tg::error!(!error, %id, region = %region, "failed to get the process"),
		)?
		else {
			return Ok(None);
		};
		self.update_tokens_and_location(
			&mut output.tokens,
			Some(&mut output.location),
			&location,
			false,
		)?;
		self.update_process_data_referents_for_location(&mut output.data, &location, false)?;
		Ok(Some(output))
	}

	async fn try_get_process_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_remote(id, remote, metadata, availability, tokens))
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

		// Spawn a task to put the process if it is finished.
		if output.data.status.is_finished() && !Self::process_log_needs_compaction(&output.data) {
			self.spawn_remote_process_put_task(
				id,
				&output.data,
				output.location.as_ref(),
				&output.tokens,
			);
		}

		Ok(Some(output))
	}

	fn spawn_remote_process_put_task(
		&self,
		id: &tg::process::Id,
		data: &tg::process::Data,
		location: Option<&tg::Location>,
		tokens: &tg::authorization::Tokens,
	) {
		let mut session = self.clone();
		session.context.stopper = None;
		self.server
			.remote_process_put_tasks
			.spawn(|_| {
				let data = data.clone();
				let id = id.clone();
				let location = location.cloned().map(Into::into);
				let tokens = tokens.clone();
				async move {
					if let Err(error) = session
						.cache_process_remote_task(&id, data, location, tokens)
						.boxed()
						.await
					{
						tracing::error!(error = %error.trace(), %id, "failed to cache the process");
					}
				}
			})
			.detach();
	}

	async fn cache_process_remote_task(
		&self,
		id: &tg::process::Id,
		mut data: tg::process::Data,
		location: Option<tg::location::Arg>,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<()> {
		let children = if let Some(children) = data.children.take() {
			children
		} else {
			let arg = tg::process::children::get::Arg {
				location,
				tokens,
				..Default::default()
			};
			self.try_get_process_children(id, arg)
				.await?
				.ok_or_else(|| tg::error!("expected the process to exist"))?
				.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
				.try_flatten()
				.try_collect()
				.await?
		};
		data.children = Some(children);
		let arg = tg::process::put::Arg {
			data,
			location: None,
		};
		Box::pin(self.put_process(id, arg)).await?;

		Ok(())
	}

	async fn try_get_process_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
		metadata: bool,
		availability: bool,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let client = self
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		let location = tg::location::Arg(vec![tg::location::arg::Component::Local(
			tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			},
		)]);
		let arg = tg::process::get::Arg {
			availability,
			location: Some(location),
			metadata,
			tokens: tokens.for_location(&tg::Location::Remote(tg::location::Remote {
				name: remote.name.clone(),
				region: None,
			})),
		};
		let Some(mut output) = client.try_get_process(id, arg).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the process"),
		)?
		else {
			return Ok(None);
		};
		let region = match output.location.take() {
			Some(tg::Location::Local(local)) => local.region,
			Some(tg::Location::Remote(remote)) => remote.region,
			None => None,
		};
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region,
		});
		self.update_tokens_and_location(
			&mut output.tokens,
			Some(&mut output.location),
			&location,
			trusted,
		)?;
		self.update_process_data_referents_for_location(&mut output.data, &location, trusted)?;
		Ok(Some(output))
	}

	pub(crate) async fn try_get_process_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the process.
		let Some(mut output) = self.try_get_process(&id, arg).await? else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.empty()
				.unwrap()
				.boxed_body());
		};
		if output.data.status.is_finished() && output.data.children.is_none() {
			let arg = tg::process::children::get::Arg {
				location: output.location.clone().map(Into::into),
				tokens: output.tokens.clone(),
				..Default::default()
			};
			if let Some(stream) = self.try_get_process_children(&id, arg).await? {
				let children = stream
					.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
					.try_flatten()
					.try_collect()
					.await?;
				output.data.children = Some(children);
			}
		}

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
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
			response = response.header(
				tg::process::get::METADATA_HEADER,
				serde_json::to_string(metadata).unwrap(),
			);
		}
		if let Some(availability) = &output.availability {
			response = response.header(
				tg::process::get::AVAILABILITY_HEADER,
				serde_json::to_string(availability).unwrap(),
			);
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

impl Server {
	pub async fn try_get_process_local(
		&self,
		id: &tg::process::Id,
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.try_get_process_batch_local(std::slice::from_ref(id), metadata)
			.await
			.map(|outputs| outputs.into_iter().next().unwrap())
	}

	pub async fn try_get_process_batch_local(
		&self,
		ids: &[tg::process::Id],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let processes = self.index.try_get_processes(ids).await?;
		let location = self.config().region.clone().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|region| {
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				})
			},
		);

		// Combine data and metadata into outputs.
		let outputs = std::iter::zip(ids, processes)
			.map(|(id, process)| {
				let process = process?;
				let data = process.data?;
				if !data.status.is_finished() {
					return None;
				}
				let data = data.without_location_and_tokens();
				let metadata = metadata.then_some(process.metadata);
				Some(tg::process::get::Output {
					availability: None,
					data,
					id: id.clone(),
					location: Some(location.clone()),
					metadata,
					tokens: tg::authorization::Tokens::default(),
				})
			})
			.collect();

		Ok(outputs)
	}
}
