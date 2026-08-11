use {
	crate::{Server, Session},
	futures::{StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

impl Session {
	pub async fn try_get_process_stored(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stored::Arg,
	) -> tg::Result<Option<tg::process::Stored>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stored) = self
					.try_get_process_stored_local(id, arg.token.as_ref())
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to get the process's storage status")
					})? {
				return Ok(Some(stored));
			}

			if let Some(stored) = self
				.try_get_process_stored_regions(id, &local.regions, arg.token.as_ref())
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						"failed to get the process's storage status from another region"
					)
				})? {
				return Ok(Some(stored));
			}
		}

		if let Some(stored) = self
			.try_get_process_stored_remotes(id, &locations.remotes, arg.token.as_ref())
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to get the process's storage status from a remote"
				)
			})? {
			return Ok(Some(stored));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_stored_local(
		&self,
		id: &tg::process::Id,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Stored>> {
		let Some(stored) = self.server.try_get_process_stored_local(id).await? else {
			return Ok(None);
		};
		self.mask_process_stored(id, stored, token).await
	}

	pub(crate) async fn mask_process_stored(
		&self,
		id: &tg::process::Id,
		stored: tg::process::Stored,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Stored>> {
		let resource = tg::Referent::with_node_and_token(id.clone(), token.cloned());
		let requested =
			tg::grant::permission::Set::Process(tg::grant::permission::process::Set::all());
		let Some(permissions) = self.authorize(resource, requested).await? else {
			return Ok(None);
		};
		Ok(Self::mask_process_stored_with_permissions(
			&stored,
			permissions,
		))
	}

	pub(crate) fn mask_process_stored_with_permissions(
		stored: &tg::process::Stored,
		permissions: tg::grant::permission::Set,
	) -> Option<tg::process::Stored> {
		let mut output = tg::process::Stored::default();
		let mut authorized = false;

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::Node,
		)) {
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::Subtree,
		)) {
			output.subtree = stored.subtree;
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::SubtreeCommand,
		)) {
			output.node_command = stored.node_command;
			output.subtree_command = stored.subtree_command;
			authorized = true;
		} else if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::NodeCommand,
		)) {
			output.node_command = stored.node_command;
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::SubtreeError,
		)) {
			output.node_error = stored.node_error;
			output.subtree_error = stored.subtree_error;
			authorized = true;
		} else if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::NodeError,
		)) {
			output.node_error = stored.node_error;
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::SubtreeLog,
		)) {
			output.node_log = stored.node_log;
			output.subtree_log = stored.subtree_log;
			authorized = true;
		} else if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::NodeLog,
		)) {
			output.node_log = stored.node_log;
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::SubtreeOutput,
		)) {
			output.node_output = stored.node_output;
			output.subtree_output = stored.subtree_output;
			authorized = true;
		} else if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::NodeOutput,
		)) {
			output.node_output = stored.node_output;
			authorized = true;
		}

		authorized.then_some(output)
	}

	async fn try_get_process_stored_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Stored>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_stored_region(id, region, token))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stored)) => {
					result = Ok(Some(stored));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		result
	}

	async fn try_get_process_stored_region(
		&self,
		id: &tg::process::Id,
		region: &str,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Stored>> {
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::stored::Arg {
			location: Some(location.into()),
			token: token.cloned(),
		};
		client.try_get_process_stored(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the process's storage status"),
		)
	}

	async fn try_get_process_stored_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Stored>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_stored_remote(id, remote, token))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stored)) => {
					result = Ok(Some(stored));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		result
	}

	async fn try_get_process_stored_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Stored>> {
		let client = self
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let arg = tg::process::stored::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			token: token.cloned(),
		};
		client.try_get_process_stored(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the process's storage status"),
		)
	}

	pub(crate) async fn try_get_process_stored_request(
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
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the process's storage status.
		let Some(output) = self.try_get_process_stored(&id, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}
}

impl Server {
	pub(crate) async fn try_get_process_stored_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Stored>> {
		Ok(self
			.index
			.try_get_process(id)
			.await?
			.map(|process| process.stored))
	}
}
