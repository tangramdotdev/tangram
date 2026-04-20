use {
	crate::{Context, Server},
	futures::{
		FutureExt as _, StreamExt as _,
		future::{self, BoxFuture},
		stream::{self, FuturesUnordered},
	},
	std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	tangram_client::prelude::*,
	tangram_futures::{future::Ext as _, stream::TryExt as _, task::Stopper},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Server {
	pub async fn try_wait_process_future_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		self.try_wait_process_future_with_context_and_stopper(context, id, arg, None)
			.await
	}

	async fn try_wait_process_future_with_context_and_stopper(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
		stopper: Option<Stopper>,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(future) = self
					.try_wait_process_local(id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to wait for the process"))?
			{
				let future =
					self.attach_wait_process_cancel_guard(id, &arg, None, stopper.clone(), future);
				return Ok(Some(future));
			}

			if let Some((future, region)) = self
				.try_wait_process_regions(id, &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to wait for the process in another region"),
				)? {
				let location = Some(tg::Location::Local(tg::location::Local {
					region: Some(region),
				}));
				let future = self.attach_wait_process_cancel_guard(
					id,
					&arg,
					location.map(Into::into),
					stopper.clone(),
					future,
				);
				return Ok(Some(future));
			}
		}

		let Some((future, remote)) = self
			.try_wait_process_remotes(id, &locations.remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to wait for the process on the remote"),
			)?
		else {
			return Ok(None);
		};
		let location = Some(
			tg::Location::Remote(tg::location::Remote {
				name: remote.remote.clone(),
				region: None,
			})
			.into(),
		);
		let future = self.attach_wait_process_cancel_guard(id, &arg, location, stopper, future);

		Ok(Some(future))
	}

	async fn try_wait_process_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		let server = self.clone();
		let id = id.clone();
		let Some(stream) = server
			.try_get_process_status_stream_local(&id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process status stream"))?
			.map(futures::StreamExt::boxed)
		else {
			return Ok(None);
		};
		let future = async move {
			let stream = stream
				.take_while(|event| {
					future::ready(!matches!(event, Ok(tg::process::status::Event::End)))
				})
				.map(|event| match event {
					Ok(tg::process::status::Event::Status(status)) => Ok(status),
					Err(error) => Err(error),
					_ => unreachable!(),
				});
			let status = stream
				.try_last()
				.await?
				.ok_or_else(|| tg::error!("failed to get the status"))?;
			if !status.is_finished() {
				return Err(tg::error!("expected the process to be finished"));
			}
			let output = server
				.try_get_process_local(&id, false)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
				.ok_or_else(|| tg::error!(%id, "failed to get the process"))?;
			let exit = output
				.data
				.exit
				.ok_or_else(|| tg::error!("expected the exit to be set"))?;
			let output = tg::process::wait::Output {
				error: output.data.error,
				exit,
				output: output.data.output,
			};
			Ok(Some(output))
		};
		Ok(Some(future.boxed()))
	}

	async fn try_wait_process_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			String,
		)>,
	> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_wait_process_region(id, region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(future)) => {
					result = Ok(Some(future));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(future) = result? else {
			return Ok(None);
		};
		Ok(Some(future))
	}

	async fn try_wait_process_region(
		&self,
		id: &tg::process::Id,
		region: &str,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			String,
		)>,
	> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::wait::Arg {
			location: Some(location.into()),
			token: None,
		};
		let Some(future) = client.try_wait_process_future(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to wait for the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some((future.boxed(), region.to_owned())))
	}

	async fn try_wait_process_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			crate::location::Remote,
		)>,
	> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_wait_process_remote(id, remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(future)) => {
					result = Ok(Some(future));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(future) = result? else {
			return Ok(None);
		};
		Ok(Some(future))
	}

	async fn try_wait_process_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
	) -> tg::Result<
		Option<(
			BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
			crate::location::Remote,
		)>,
	> {
		let client = self
			.get_remote_client(remote.remote.clone())
			.await
			.map_err(
				|source| tg::error!(!source, remote = %remote.remote, "failed to get the remote client"),
			)?;
		let arg = tg::process::wait::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			token: None,
		};
		let Some(future) = client.try_wait_process_future(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.remote, "failed to wait for the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some((future.boxed(), remote.clone())))
	}

	fn attach_wait_process_cancel_guard(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::wait::Arg,
		location: Option<tg::location::Arg>,
		stopper: Option<Stopper>,
		future: BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
	) -> BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>> {
		// If a token is provided, attach a cancellation guard.
		if let Some(token) = arg.token.clone() {
			let cancel = Arc::new(AtomicBool::new(true));
			let future = {
				let cancel = cancel.clone();
				async move {
					let output = future.await;
					cancel.store(false, Ordering::SeqCst);
					output
				}
			}
			.boxed();

			let server = self.clone();
			let id = id.clone();
			let guard = scopeguard::guard((), move |()| {
				if cancel.load(Ordering::SeqCst) && !stopper.as_ref().is_some_and(Stopper::stopped)
				{
					let arg = tg::process::cancel::Arg {
						location: location.clone(),
						token,
					};
					tokio::spawn(async move {
						server.cancel_process(&id, arg).await.ok();
					});
				}
			});

			future.attach(guard).boxed()
		} else {
			future
		}
	}

	pub(crate) async fn handle_post_process_wait_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Parse the arg.
		let arg = request
			.query_params::<tg::process::wait::Arg>()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the stopper.
		let stopper = request.extensions().get::<Stopper>().cloned().unwrap();

		// Get the future.
		let Some(future) = self
			.try_wait_process_future_with_context_and_stopper(
				context,
				&id,
				arg,
				Some(stopper.clone()),
			)
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the stream.
		let stream = stream::once(future).filter_map(|result| async move {
			match result {
				Ok(Some(value)) => Some(Ok(tg::process::wait::Event::Output(value))),
				Ok(None) => None,
				Err(error) => Some(Err(error)),
			}
		});

		// Stop the stream when the server stops.
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
