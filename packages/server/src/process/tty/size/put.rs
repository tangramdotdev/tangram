use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> tg::Result<Option<()>> {
		let location = self.server.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_set_process_tty_size_local(id, arg.size, arg.token.as_ref())
					.await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_set_process_tty_size_region(id, arg.size, region, arg.token.as_ref())
					.await?
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.try_set_process_tty_size_remote(
					id,
					arg.size,
					remote,
					region,
					arg.token.as_ref(),
				)
				.await?
			},
		};

		Ok(output)
	}

	async fn try_set_process_tty_size_local(
		&self,
		id: &tg::process::Id,
		size: tg::process::tty::Size,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<()>> {
		let Some(output) = self
			.try_get_process_local(id, false, token)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
		else {
			return Ok(None);
		};

		// Check if the process has a tty.
		if output.data.tty.is_none() {
			return Err(tg::error!(%id, "the process does not have a tty associated with it"));
		}
		if output.data.status.is_finished() {
			return Ok(Some(()));
		}

		// Send the control request.
		let request = tg::process::control::ServerRequestArg::Tty(
			tg::process::control::TtyServerRequestArg { size },
		);
		let retry = tangram_futures::retry::Options::default();
		let timeout = self
			.server
			.config
			.runner
			.as_ref()
			.map_or(std::time::Duration::from_secs(10), |runner| {
				runner.stdio_drain_timeout
			});
		let options = crate::control::Options { retry, timeout };
		let response = self
			.send_process_control_request(id, request, options)
			.await??;
		response
			.try_unwrap_tty()
			.map_err(|_| tg::error!("expected a tty response"))?;

		Ok(Some(()))
	}

	async fn try_set_process_tty_size_region(
		&self,
		id: &tg::process::Id,
		size: tg::process::tty::Size,
		region: String,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::tty::size::put::Arg {
			location: Some(location.into()),
			size,
			token: token.cloned(),
		};
		let Some(()) = client.try_set_process_tty_size(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to put the process tty"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_set_process_tty_size_remote(
		&self,
		id: &tg::process::Id,
		size: tg::process::tty::Size,
		remote: String,
		region: Option<String>,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::tty::size::put::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			size,
			token: token.cloned(),
		};
		let Some(()) = client.try_set_process_tty_size(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to put the process tty"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn try_set_process_tty_size_request(
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

		// Put the process tty.
		let Some(()) = self
			.try_set_process_tty_size(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the process tty"))?
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
}
