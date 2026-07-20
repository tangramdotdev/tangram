use {
	crate::{Server, Session},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{self, FuturesUnordered},
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
					.try_get_process_local(id, arg.metadata, arg.token.as_ref())
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_get_process_regions(id, &local.regions, arg.metadata, arg.token.as_ref())
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the process from another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_get_process_remotes(id, &locations.remotes, arg.metadata, arg.token.as_ref())
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process from a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_local(
		&self,
		id: &tg::process::Id,
		metadata: bool,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let resource = tg::Referent::with_item_and_token(id.clone(), token.cloned());
		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Node);
		let authorize_future = async {
			let authorized = self.authorize(resource, permission).await?;
			Ok::<_, tg::Error>(
				authorized.is_some_and(|permissions| permissions.contains(permission)),
			)
		}
		.boxed();
		let get_future = self.try_get_process_local_inner(id, metadata).boxed();
		let (authorized, output) = future::try_join(authorize_future, get_future).await?;
		if !authorized {
			return Ok(None);
		}
		let Some(mut output) = output else {
			return Ok(None);
		};
		if let Some(metadata) = output.metadata.take() {
			output.metadata = self
				.mask_process_metadata(id, metadata, token)
				.boxed()
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

	pub(crate) async fn try_get_process_local_inner(
		&self,
		id: &tg::process::Id,
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		if let Some(data) = self.server.runner.state.try_get_process(id)
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
					let data = control_future.await?;
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
				let data = data?;
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
		let data = data.without_tokens();
		let location = self.server.config().region.clone().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|region| {
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				})
			},
		);
		tg::process::get::Output {
			data,
			id: id.clone(),
			location: Some(location),
			metadata,
		}
	}

	async fn try_get_process_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		metadata: bool,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_region(id, region, metadata, token))
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
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::get::Arg {
			location: Some(location.clone().into()),
			metadata,
			token: token.cloned(),
		};
		let Some(mut output) = client.try_get_process(id, arg).await.map_err(
			|error| tg::error!(!error, %id, region = %region, "failed to get the process"),
		)?
		else {
			return Ok(None);
		};
		output.location = Some(location);
		Ok(Some(output))
	}

	async fn try_get_process_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
		metadata: bool,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_remote(id, remote, metadata, token))
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
			tokio::spawn({
				let session = self.clone();
				let id = id.clone();
				let mut data = output.data.clone();
				async move {
					let arg = tg::process::children::get::Arg::default();
					let children = session
						.try_get_process_children(&id, arg)
						.await?
						.ok_or_else(|| tg::error!("expected the process to exist"))?
						.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
						.try_flatten()
						.try_collect()
						.await?;
					data.children = Some(children);
					let arg = tg::process::put::Arg {
						data,
						location: None,
					};
					session.put_process(&id, arg).await?;
					Ok::<_, tg::Error>(())
				}
			});
		}

		Ok(Some(output))
	}

	async fn try_get_process_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
		metadata: bool,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let location = tg::location::Arg(vec![tg::location::arg::Component::Local(
			tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			},
		)]);
		let arg = tg::process::get::Arg {
			location: Some(location),
			metadata,
			token: token.cloned(),
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
		output.location = Some(tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region,
		}));
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
		let Some(output) = self.try_get_process(&id, arg).await? else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.empty()
				.unwrap()
				.boxed_body());
		};

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
				let data = data.without_tokens();
				let metadata = metadata.then_some(process.metadata);
				Some(tg::process::get::Output {
					data,
					id: id.clone(),
					location: Some(location.clone()),
					metadata,
				})
			})
			.collect();

		Ok(outputs)
	}
}
