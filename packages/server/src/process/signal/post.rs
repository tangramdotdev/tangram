use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
};

impl Session {
	pub(crate) async fn try_post_process_signal(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_post_process_signal_local(id, arg.signal, &arg.lease)
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to signal the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_post_process_signal_regions(id, arg.signal, &arg.lease, &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to signal the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_post_process_signal_remotes(id, arg.signal, &arg.lease, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to signal the process in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_post_process_signal_local(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		lease: &str,
	) -> tg::Result<Option<()>> {
		let Some(output) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
		else {
			return Ok(None);
		};
		let cacheable = output.data.cacheable;

		// Check if the process is cacheable.
		if cacheable {
			return Err(tg::error!(%id, "cannot signal cacheable processes"));
		}
		self.authorize_process_lease(id, Some(lease)).await?;

		// Insert the signal into the process store.
		let process = id.to_string();
		self.server
			.process_store
			.run(|transaction| {
				let process = process.clone();
				async move {
					Self::post_process_signal_with_transaction(transaction, &process, signal).await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to insert the process signal"))?;

		// Publish the signal message.
		self.spawn_publish_process_signal_message_task(id);

		Ok(Some(()))
	}

	async fn post_process_signal_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		process: &str,
		signal: tg::process::Signal,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into process_signals (process, signal)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![process, signal.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}

	async fn try_post_process_signal_regions(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		lease: &str,
		regions: &[String],
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_post_process_signal_region(id, signal, lease, region))
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
		lease: &str,
		region: &str,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::signal::post::Arg {
			lease: lease.to_owned(),
			location: Some(location.into()),
			signal,
		};
		let Some(()) = client.try_post_process_signal(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to signal the process"),
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
		lease: &str,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_post_process_signal_remote(id, signal, lease, remote))
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
		lease: &str,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::signal::post::Arg {
			lease: lease.to_owned(),
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			signal,
		};
		let Some(()) = client.try_post_process_signal(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to signal the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn try_signal_process_request(
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
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Post the process signal.
		let Some(()) = self
			.try_post_process_signal(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to post process signal"))?
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
			let session = self.clone();
			async move {
				session
					.server
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
