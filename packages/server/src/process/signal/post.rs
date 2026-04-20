use {
	crate::{Context, Server},
	futures::{StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn try_post_process_signal_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_post_process_signal_local(id, arg.signal)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to signal the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_post_process_signal_regions(id, arg.signal, &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to signal the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_post_process_signal_remotes(id, arg.signal, &locations.remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to signal the process in a remote"),
			)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_post_process_signal_local(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
	) -> tg::Result<Option<()>> {
		let Some(output) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
		else {
			return Ok(None);
		};
		let cacheable = output.data.cacheable;

		// Check if the process is cacheable.
		if cacheable {
			return Err(tg::error!(%id, "cannot signal cacheable processes"));
		}

		// Insert the signal into the process store.
		let connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into process_signals (process, signal)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![id.to_string(), signal.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		// Publish the signal message.
		self.spawn_publish_process_signal_message_task(id);

		Ok(Some(()))
	}

	async fn try_post_process_signal_regions(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		regions: &[String],
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_post_process_signal_region(id, signal, region))
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

	async fn try_post_process_signal_region(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		region: &str,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::signal::post::Arg {
			signal,
			location: Some(location.into()),
		};
		let Some(()) = client.try_post_process_signal(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to signal the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_post_process_signal_remotes(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_post_process_signal_remote(id, signal, remote))
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

	async fn try_post_process_signal_remote(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_remote_client(remote.remote.clone())
			.await
			.map_err(
				|source| tg::error!(!source, remote = %remote.remote, %id, "failed to get the remote client"),
			)?;
		let arg = tg::process::signal::post::Arg {
			signal,
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let Some(()) = client.try_post_process_signal(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.remote, "failed to signal the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn handle_post_process_signal_request(
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
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Post the process signal.
		let Some(()) = self
			.try_post_process_signal_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post process signal"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) fn spawn_publish_process_signal_message_task(&self, id: &tg::process::Id) {
		let id = id.clone();
		let subject = format!("processes.{id}.signal");
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish(subject, ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, %id, "failed to publish the process signal message");
					})
					.ok();
			}
		});
	}
}
