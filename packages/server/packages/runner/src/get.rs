use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn try_get_runner(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		mut arg: tg::runner::get::Arg,
	) -> tg::Result<Option<tg::runner::get::Output>> {
		let location = session.server.runner_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_get_runner(runner, arg).await
			},
			tg::Location::Local(_) => self.try_get_runner_local(session, runner).await,
			tg::Location::Remote(remote) => {
				let client = session
					.server
					.components
					.remote
					.get_remote_session(session, &remote.name)
					.await?;
				arg.location = Some(
					tg::Location::Local(tg::location::Local {
						region: remote.region.clone(),
					})
					.into(),
				);
				let location = tg::Location::Remote(tg::location::Remote {
					name: remote.name.clone(),
					region: remote.region,
				});
				let output = client.try_get_runner(runner, arg).await?.map(|mut output| {
					output.location = Some(location);
					output
				});
				Ok(output)
			},
		}
	}

	async fn try_get_runner_local(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
	) -> tg::Result<Option<tg::runner::get::Output>> {
		// Get the runner, and treat a runner the session may not manage as absent.
		let Some(data) = self.try_get_runner_data(session, runner).await? else {
			return Ok(None);
		};
		let owner = data.owner.as_ref().and_then(tg::Principal::to_id);
		let authorized = self
			.is_runner_owner_authorized(session, owner.as_ref())
			.await?;
		if !authorized {
			return Ok(None);
		}

		// Create the output.
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let output = tg::runner::get::Output {
			data,
			location: Some(location),
		};

		Ok(Some(output))
	}

	pub(crate) async fn try_get_runner_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		runner: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the runner ID"))?;
		let Some(output) = self.try_get_runner(session, &runner, arg).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let body = serde_json::to_vec(&output).unwrap();
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(BoxBody::with_bytes(body))
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
