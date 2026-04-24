use {
	crate::{Context, Server, database::Database},
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, FuturesOrdered, FuturesUnordered},
	},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub async fn try_get_process_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_process_local(id, arg.metadata)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_get_process_regions(id, &local.regions, arg.metadata)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to get the process from another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_get_process_remotes(id, &locations.remotes, arg.metadata)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process from a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_local(
		&self,
		id: &tg::process::Id,
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.try_get_process_batch_local(std::slice::from_ref(id), metadata)
			.await
			.map(|outputs| outputs.into_iter().next().unwrap())
	}

	pub async fn try_get_process_batch(
		&self,
		ids: &[tg::process::Id],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let outputs = self
			.try_get_process_batch_local(ids, metadata)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes locally"))?;
		let locations = self
			.locations(None)
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;
		let regions = locations.local.map_or_else(Vec::new, |local| local.regions);
		let remotes = locations.remotes;
		let outputs = std::iter::zip(ids, outputs)
			.map(|(id, output)| {
				let regions = regions.clone();
				let remotes = remotes.clone();
				async move {
					if let Some(output) = output {
						return Ok(Some(output));
					}

					if let Some(output) =
						self.try_get_process_regions(id, &regions, metadata).await?
					{
						return Ok(Some(output));
					}

					if let Some(output) =
						self.try_get_process_remotes(id, &remotes, metadata).await?
					{
						return Ok(Some(output));
					}

					Ok::<_, tg::Error>(None)
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	pub async fn try_get_process_batch_local(
		&self,
		ids: &[tg::process::Id],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		// Get the process from the process store.
		let data_future = async {
			match &self.process_store {
				#[cfg(feature = "postgres")]
				Database::Postgres(process_store) => {
					self.try_get_process_batch_postgres(process_store, ids)
						.await
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(process_store) => self.try_get_process_batch_sqlite(process_store, ids).await,
			}
		};

		// Get the metadata if requested.
		let metadata_future = async {
			if metadata {
				self.try_get_process_metadata_batch_local(ids)
					.await
					.unwrap_or_else(|_| vec![None; ids.len()])
			} else {
				vec![None; ids.len()]
			}
		};

		// Fetch the data and metadata concurrently.
		let (data, metadata) = future::join(data_future, metadata_future).await;
		let data = data?;
		let location = self.config().region.clone().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|region| {
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				})
			},
		);

		// Combine data and metadata into outputs.
		let outputs = std::iter::zip(data, metadata)
			.map(|(output, metadata)| {
				output.map(|mut output| {
					output.location = Some(location.clone());
					output.metadata = metadata;
					output
				})
			})
			.collect();

		Ok(outputs)
	}

	async fn try_get_process_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_region(id, region, metadata))
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
	) -> tg::Result<Option<tg::process::get::Output>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::get::Arg {
			location: Some(location.clone().into()),
			metadata,
		};
		let Some(mut output) = client.try_get_process(id, arg).await.map_err(
			|source| tg::error!(!source, %id, region = %region, "failed to get the process"),
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
	) -> tg::Result<Option<tg::process::get::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_remote(id, remote, metadata))
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
		if output.data.status.is_finished() {
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				let mut data = output.data.clone();
				async move {
					let arg = tg::process::children::get::Arg::default();
					let children = server
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
					server.put_process(&id, arg).await?;
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
	) -> tg::Result<Option<tg::process::get::Output>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to get the remote client"),
		)?;
		let location = tg::location::Arg(vec![tg::location::arg::Component::Local(
			tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			},
		)]);
		let arg = tg::process::get::Arg {
			location: Some(location),
			metadata,
		};
		let Some(mut output) = client.try_get_process(id, arg).await.map_err(
			|source| tg::error!(!source, %id, remote = %remote.name, "failed to get the process"),
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

	pub(crate) async fn handle_get_process_request(
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

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the process.
		let Some(output) = self.try_get_process_with_context(context, &id, arg).await? else {
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
