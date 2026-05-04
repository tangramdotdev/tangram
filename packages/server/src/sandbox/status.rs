use {
	crate::{Context, Server},
	futures::{
		StreamExt as _,
		stream::{self, BoxStream, FuturesUnordered},
	},
	indoc::formatdoc,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{
		stream::Ext as _,
		task::{Stopper, Task},
	},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

impl Server {
	pub async fn try_get_sandbox_status_stream_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<
		Option<
			impl futures::Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static + use<>,
		>,
	> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(status) = self
					.try_get_sandbox_status_stream_local(id, context.stopper.clone())
					.await
					.map_err(
						|source| tg::error!(!source, %id, "failed to get the sandbox status stream"),
					)? {
				return Ok(Some(status));
			}

			if let Some(status) = self
				.try_get_sandbox_status_stream_regions(id, &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to get the sandbox status from another region"),
				)? {
				return Ok(Some(status));
			}
		}

		if let Some(status) = self
			.try_get_sandbox_status_stream_remotes(id, &locations.remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to get the sandbox status from a remote"),
			)? {
			return Ok(Some(status));
		}

		Ok(None)
	}

	async fn try_get_sandbox_status_stream_local(
		&self,
		id: &tg::sandbox::Id,
		stopper: Option<Stopper>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		if !self
			.get_sandbox_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to check if the sandbox exists"))?
		{
			return Ok(None);
		}

		let subject = format!("sandboxes.{id}.status");
		let wakeups = self
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());

		let interval = IntervalStream::new(tokio::time::interval(Duration::from_mins(1)))
			.skip(1)
			.map(|_| ());

		let wakeups = stream::select(wakeups, interval).with_stopper(stopper);

		let (sender, receiver) = tokio::sync::mpsc::channel(1);

		let server = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = server
				.try_get_sandbox_status_stream_local_task(&id, sender.clone(), wakeups)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(Some(stream))
	}

	async fn try_get_sandbox_status_stream_local_task(
		&self,
		id: &tg::sandbox::Id,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::status::Event>>,
		mut wakeups: BoxStream<'static, ()>,
	) -> tg::Result<()> {
		let mut previous: Option<tg::sandbox::Status> = None;
		loop {
			let status = self
				.try_get_sandbox_status_local(id)
				.await?
				.unwrap_or(tg::sandbox::Status::Finished);
			if previous != Some(status) {
				previous.replace(status);
				let event = tg::sandbox::status::Event::Status(status);
				if sender.send(Ok(event)).await.is_err() {
					return Ok(());
				}
			}
			if status.is_finished() {
				sender.send(Ok(tg::sandbox::status::Event::End)).await.ok();
				return Ok(());
			}
			if wakeups.next().await.is_none() {
				return Ok(());
			}
		}
	}

	pub(crate) async fn get_sandbox_status_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<tg::sandbox::Status> {
		self.try_get_sandbox_status_local(id)
			.await?
			.ok_or_else(|| tg::error!("failed to find the sandbox"))
	}

	pub(crate) async fn try_get_sandbox_status_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::Status>> {
		let connection =
			self.process_store.connection().await.map_err(|source| {
				tg::error!(!source, "failed to get a process store connection")
			})?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select status
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let Some(status) = connection
			.query_optional_value_into::<db::value::Serde<tg::sandbox::Status>>(
				statement.into(),
				params,
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|value| value.0)
		else {
			return Ok(None);
		};
		drop(connection);
		Ok(Some(status))
	}

	async fn try_get_sandbox_status_stream_regions(
		&self,
		id: &tg::sandbox::Id,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_sandbox_status_stream_region(id, region))
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

	async fn try_get_sandbox_status_stream_region(
		&self,
		id: &tg::sandbox::Id,
		region: &str,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::status::Arg {
			location: Some(location.into()),
		};
		let Some(stream) = client
			.try_get_sandbox_status_stream(id, arg)
			.await
			.map_err(
				|source| tg::error!(!source, region = %region, "failed to get the sandbox status"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	async fn try_get_sandbox_status_stream_remotes(
		&self,
		id: &tg::sandbox::Id,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_sandbox_status_stream_remote(id, remote))
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

	async fn try_get_sandbox_status_stream_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, %id, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::status::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let Some(stream) = client
			.try_get_sandbox_status_stream(id, arg)
			.await
			.map_err(
				|source| tg::error!(!source, %id, remote = %remote.name, "failed to get the sandbox status"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	pub(crate) async fn handle_get_sandbox_status_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;
		let Some(stream) = self
			.try_get_sandbox_status_stream_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
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
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap())
	}
}
