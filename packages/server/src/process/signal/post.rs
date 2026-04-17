use {
	crate::{Context, Server},
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
		let location = self.location_with_regions(arg.location.as_ref())?;

		let output = match location {
			crate::location::Location::Local { region: None } => {
				self.try_post_process_signal_local(id, arg.signal).await?
			},
			crate::location::Location::Local {
				region: Some(region),
			} => {
				self.try_post_process_signal_region(id, arg.signal, region)
					.await?
			},
			crate::location::Location::Remote { remote, region } => {
				self.try_post_process_signal_remote(id, arg.signal, remote, region)
					.await?
			},
		};

		Ok(output)
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

		// Insert the signal into the sandbox store.
		let connection = self
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
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

	async fn try_post_process_signal_region(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		region: String,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, region = %region, %id, "failed to get the region client"),
		)?;
		let arg = tg::process::signal::post::Arg {
			signal,
			location: Some(tg::location::Location::Local(tg::location::Local {
				regions: Some(vec![region.clone()]),
			})),
		};
		let Some(()) = client.try_post_process_signal(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to signal the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_post_process_signal_remote(
		&self,
		id: &tg::process::Id,
		signal: tg::process::Signal,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_client(remote.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::signal::post::Arg {
			signal,
			location: Some(tg::location::Location::Local(tg::location::Local {
				regions: region.map(|region| vec![region]),
			})),
		};
		let Some(()) = client.try_post_process_signal(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote, "failed to signal the process"),
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
