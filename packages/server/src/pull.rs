use {
	crate::{Context, Server},
	futures::{Stream, StreamExt as _, future, stream},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

impl Server {
	pub(crate) async fn pull_with_context(
		&self,
		_context: &Context,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + use<>,
	> {
		if !arg.force
			&& self.pull_items_stored(&arg).await.map_err(|source| {
				tg::error!(!source, "failed to check whether the pull is local")
			})? {
			let stream = stream::once(future::ok(tg::progress::Event::Output(
				tg::pull::Output::default(),
			)));
			return Ok(stream.boxed());
		}

		let source = arg.source.clone().unwrap_or_else(|| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		let destination = arg
			.destination
			.clone()
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let arg: tg::push::Arg = arg.clone().into();
		let stream = self.push_or_pull(&arg, source, destination).await?;
		Ok(stream.boxed())
	}

	async fn pull_items_stored(&self, arg: &tg::pull::Arg) -> tg::Result<bool> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let object_ids = arg
			.items
			.iter()
			.filter_map(|item| match item {
				tg::Either::Left(object) => Some(object.clone()),
				tg::Either::Right(_) => None,
			})
			.collect::<Vec<_>>();
		let process_ids = arg
			.items
			.iter()
			.filter_map(|item| match item {
				tg::Either::Left(_) => None,
				tg::Either::Right(process) => Some(process.clone()),
			})
			.collect::<Vec<_>>();
		let touch_objects_future = async {
			self.index
				.touch_objects(&object_ids, touched_at, self.config.object.time_to_touch)
				.await
				.map_err(|source| tg::error!(!source, "failed to touch the objects"))
		};
		let touch_processes_future = async {
			self.index
				.touch_processes(&process_ids, touched_at, self.config.process.time_to_touch)
				.await
				.map_err(|source| tg::error!(!source, "failed to touch the processes"))
		};
		let (objects, processes) =
			futures::try_join!(touch_objects_future, touch_processes_future)?;
		let objects_stored = objects
			.into_iter()
			.all(|object| object.is_some_and(|object| object.stored.subtree));
		let processes_stored = processes.into_iter().all(|process| {
			let Some(process) = process else {
				return false;
			};
			let stored = process.stored;
			if arg.recursive {
				stored.subtree
					&& (!arg.commands || stored.subtree_command)
					&& (!arg.errors || stored.subtree_error)
					&& (!arg.logs || stored.subtree_log)
					&& (!arg.outputs || stored.subtree_output)
			} else {
				(!arg.commands || stored.node_command)
					&& (!arg.errors || stored.node_error)
					&& (!arg.logs || stored.node_log)
					&& (!arg.outputs || stored.node_output)
			}
		});
		let stored = objects_stored && processes_stored;
		Ok(stored)
	}

	pub(crate) async fn handle_pull_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.pull_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the pull"))?;

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
