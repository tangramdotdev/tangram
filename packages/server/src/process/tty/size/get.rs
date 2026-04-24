use {
	crate::{Context, Server},
	futures::{
		StreamExt as _,
		stream::{BoxStream, FuturesUnordered},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn try_get_process_tty_size_stream_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::tty::size::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::tty::size::get::Event>>>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stream) = self
					.try_get_process_tty_size_stream_local(id)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the process tty stream"))?
			{
				return Ok(Some(stream));
			}

			if let Some(stream) = self
				.try_get_process_tty_size_stream_regions(id, &local.regions)
				.await
				.map_err(|source| {
					tg::error!(
						!source,
						"failed to get the process tty stream from another region"
					)
				})? {
				return Ok(Some(stream));
			}
		}

		if let Some(stream) = self
			.try_get_process_tty_size_stream_remotes(id, &locations.remotes)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to get the process tty stream from a remote"
				)
			})? {
			return Ok(Some(stream));
		}

		Ok(None)
	}

	async fn try_get_process_tty_size_stream_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::tty::size::get::Event>>>> {
		// Check if the process exists locally.
		if self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
			.is_none()
		{
			return Ok(None);
		}

		let stream = self
			.messenger
			.subscribe::<tangram_messenger::payload::Json<tg::process::tty::size::get::Event>>(
				format!("processes.{id}.tty"),
				None,
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?
			.map(|result| {
				let message =
					result.map_err(|source| tg::error!(!source, "failed to get message"))?;
				Ok(message.payload.0)
			})
			.boxed();

		Ok(Some(stream))
	}

	async fn try_get_process_tty_size_stream_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::tty::size::get::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_tty_size_stream_region(id, region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stream)) => {
					result = Ok(Some(stream));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(stream) = result? else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	async fn try_get_process_tty_size_stream_region(
		&self,
		id: &tg::process::Id,
		region: &str,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::tty::size::get::Event>>>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::tty::size::get::Arg {
			location: Some(location.into()),
		};
		let Some(stream) = client
			.try_get_process_tty_size_stream(id, arg)
			.await
			.map_err(
				|source| tg::error!(!source, region = %region, "failed to get the process tty stream"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	async fn try_get_process_tty_size_stream_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::tty::size::get::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_tty_size_stream_remote(id, remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stream)) => {
					result = Ok(Some(stream));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(stream) = result? else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	async fn try_get_process_tty_size_stream_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::tty::size::get::Event>>>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::process::tty::size::get::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let Some(stream) = client
			.try_get_process_tty_size_stream(id, arg)
			.await
			.map_err(
				|source| tg::error!(!source, remote = %remote.name, "failed to get the process tty stream"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	pub(crate) async fn handle_get_process_tty_size_request(
		&self,
		request: http::Request<BoxBody>,
		_context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the args.
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
		let Some(stream) = self.try_get_process_tty_size_stream(&id, arg).await? else {
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
