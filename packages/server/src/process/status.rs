use {
	crate::{Context, Server},
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	indoc::formatdoc,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Stopper},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

impl Server {
	pub async fn try_get_process_status_stream_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(status) = self.try_get_process_status_stream_local(id).await.map_err(
				|source| tg::error!(!source, %id, "failed to get the process status stream"),
			)? {
			return Ok(Some(status));
		}

		// Try peers.
		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if let Some(status) = self.try_get_process_status_peer(id, &peers).await.map_err(
			|source| tg::error!(!source, %id, "failed to get the process status from the peer"),
		)? {
			return Ok(Some(status));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(status) = self
			.try_get_process_status_remote(id, &remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to get the process status from the remote"),
			)? {
			return Ok(Some(status));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_status_stream_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		// Verify the process is local.
		if !self
			.get_process_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to check if the process exists"))?
		{
			return Ok(None);
		}

		// Subscribe to status events.
		let subject = format!("processes.{id}.status");
		let status = self
			.messenger
			.subscribe::<()>(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());

		// Create the interval.
		let interval =
			IntervalStream::new(tokio::time::interval(Duration::from_mins(1))).map(|_| ());

		// Create the stream.
		let server = self.clone();
		let id = id.clone();
		let mut previous: Option<tg::process::Status> = None;
		let stream = stream::select(status, interval)
			.boxed()
			.then(move |()| {
				let server = server.clone();
				let id = id.clone();
				async move { server.get_process_status_local(&id).await }
			})
			.try_filter(move |status| {
				future::ready(match (previous.as_mut(), *status) {
					(None, status) => {
						previous.replace(status);
						true
					},
					(Some(previous), status) if *previous == status => false,
					(Some(previous), status) => {
						*previous = status;
						true
					},
				})
			})
			.take_while_inclusive(move |result| {
				if let Ok(status) = result {
					future::ready(!status.is_finished())
				} else {
					future::ready(false)
				}
			})
			.map_ok(tg::process::status::Event::Status)
			.chain(stream::once(future::ok(tg::process::status::Event::End)));

		Ok(Some(stream.boxed()))
	}

	pub(crate) async fn get_process_status_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tg::process::Status> {
		self.try_get_process_status_local(id)
			.await?
			.ok_or_else(|| tg::error!("failed to find the process"))
	}

	pub(crate) async fn try_get_process_status_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Status>> {
		// Get a sandbox store connection.
		let connection =
			self.sandbox_store.connection().await.map_err(|source| {
				tg::error!(!source, "failed to get a sandbox store connection")
			})?;

		// Get the status.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select status
				from processes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let Some(status) = connection
			.query_optional_value_into::<db::value::Serde<tg::process::Status>>(
				statement.into(),
				params,
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|value| value.0)
		else {
			return Ok(None);
		};

		// Drop the sandbox store connection.
		drop(connection);

		Ok(Some(status))
	}

	async fn try_get_process_status_peer(
		&self,
		id: &tg::process::Id,
		peers: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		if peers.is_empty() {
			return Ok(None);
		}

		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to get the peer client"),
			)?;
			let stream = client
				.try_get_process_status_stream(id, tg::process::status::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, peer = %peer, "failed to get the process status"),
				)?
				.map(futures::StreamExt::boxed);
			if let Some(stream) = stream {
				return Ok(Some(stream));
			}
		}

		Ok(None)
	}

	async fn try_get_process_status_remote(
		&self,
		id: &tg::process::Id,
		remotes: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		if remotes.is_empty() {
			return Ok(None);
		}

		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to get the remote client"),
			)?;
			let stream = client
				.try_get_process_status_stream(id, tg::process::status::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, remote = %remote, "failed to get the process status"),
				)?
				.map(futures::StreamExt::boxed);
			if let Some(stream) = stream {
				return Ok(Some(stream));
			}
		}

		Ok(None)
	}

	pub(crate) async fn handle_get_process_status_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self
			.try_get_process_status_stream_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Stop the stream when the server stops.
		let stopper = request.extensions().get::<Stopper>().cloned().unwrap();
		let stopper = async move { stopper.wait().await };
		let stream = stream.take_until(stopper);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
