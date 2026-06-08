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
				self.try_set_process_tty_size_local(id, arg.size).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_set_process_tty_size_region(id, arg.size, region)
					.await?
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.try_set_process_tty_size_remote(id, arg.size, remote, region)
					.await?
			},
		};

		Ok(output)
	}

	async fn try_set_process_tty_size_local(
		&self,
		id: &tg::process::Id,
		size: tg::process::tty::Size,
	) -> tg::Result<Option<()>> {
		let Some(output) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
		else {
			return Ok(None);
		};
		let has_tty = output.data.tty.is_some();

		// Check if the process has a tty.
		if !has_tty {
			return Err(tg::error!(%id, "the process does not have a tty associated with it"));
		}

		// Send the control request.
		let request =
			tg::process::control::RequestKind::Tty(tg::process::control::TtyRequest { size });
		let response = self
			.try_send_process_control_request(id, request, u64::MAX)
			.await?;
		let tg::process::control::ResponseKind::Tty = response.kind else {
			return Err(tg::error!("expected a tty response"));
		};

		Ok(Some(()))
	}

	async fn try_set_process_tty_size_region(
		&self,
		id: &tg::process::Id,
		size: tg::process::tty::Size,
		region: String,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::tty::size::put::Arg {
			size,
			location: Some(location.into()),
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
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::tty::size::put::Arg {
			size,
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
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
