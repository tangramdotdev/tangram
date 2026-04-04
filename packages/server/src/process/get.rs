use {
	crate::{Context, Server, database::Database},
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, FuturesUnordered},
	},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
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
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(output) = self
				.try_get_process_local(id, arg.metadata)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
		{
			return Ok(Some(output));
		}

		// Try peers.
		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if let Some(output) = self
			.try_get_process_peer(id, &peers, arg.metadata)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process from the peer"))?
		{
			return Ok(Some(output));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(output) = self
			.try_get_process_remote(id, &remotes, arg.metadata)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to get the process from the remote"),
			)? {
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
		let peers = self
			.peers(None, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		let remotes = self
			.remotes(None, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let outputs = std::iter::zip(ids, outputs)
			.map(|(id, output)| {
				let peers = peers.clone();
				let remotes = remotes.clone();
				async move {
					if let Some(output) = output {
						return Ok(Some(output));
					}
					let output = self.try_get_process_peer(id, &peers, metadata).await?;
					if output.is_some() {
						return Ok(output);
					}
					let output = self.try_get_process_remote(id, &remotes, metadata).await?;
					Ok::<_, tg::Error>(output)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	pub async fn try_get_process_batch_local(
		&self,
		ids: &[tg::process::Id],
		metadata: bool,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		// Get the process from the register.
		let data_future = async {
			match &self.register {
				#[cfg(feature = "postgres")]
				Database::Postgres(register) => self.try_get_process_batch_postgres(register, ids).await,
				#[cfg(feature = "sqlite")]
				Database::Sqlite(register) => self.try_get_process_batch_sqlite(register, ids).await,
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

		// Combine data and metadata into outputs.
		let outputs = std::iter::zip(data, metadata)
			.map(|(output, metadata)| {
				output.map(|mut output| {
					output.metadata = metadata;
					output
				})
			})
			.collect();

		Ok(outputs)
	}

	async fn try_get_process_peer(
		&self,
		id: &tg::process::Id,
		peers: &[String],
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		if peers.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::get::Arg {
			metadata,
			..Default::default()
		};
		let mut output = None;
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, peer = %peer, "failed to get the peer client"),
			)?;
			let peer_output = client.try_get_process(id, arg.clone()).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to get the process"),
			)?;
			if let Some(peer_output) = peer_output {
				output = Some(peer_output);
				break;
			}
		}
		let Some(output) = output else {
			return Ok(None);
		};

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
						local: None,
						remotes: None,
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
		remotes: &[String],
		metadata: bool,
	) -> tg::Result<Option<tg::process::get::Output>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::get::Arg {
			metadata,
			..Default::default()
		};
		let mut output = None;
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, remote = %remote, "failed to get the remote client"),
			)?;
			let remote_output = client.try_get_process(id, arg.clone()).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to get the process"),
			)?;
			if let Some(remote_output) = remote_output {
				output = Some(remote_output);
				break;
			}
		}
		let Some(output) = output else {
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
						local: None,
						remotes: None,
					};
					server.put_process(&id, arg).await?;
					Ok::<_, tg::Error>(())
				}
			});
		}

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
				.body(BoxBody::empty())
				.unwrap());
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
