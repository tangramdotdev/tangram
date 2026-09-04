use {
	crate::Session,
	futures::{StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub async fn try_get_process_availability(
		&self,
		id: &tg::process::Id,
		arg: tg::process::availability::Arg,
	) -> tg::Result<Option<tg::process::Availability>> {
		let locations = self
			.process_locations(id, arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(availability) = self
					.try_get_process_availability_local(id, arg.tokens.local())
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to get the process's availability")
					})? {
				return Ok(Some(availability));
			}

			if let Some(availability) = self
				.try_get_process_availability_regions(id, &local.regions, &arg.tokens)
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						"failed to get the process's availability from another region"
					)
				})? {
				return Ok(Some(availability));
			}
		}

		if let Some(availability) = self
			.try_get_process_availability_remotes(id, &locations.remotes, &arg.tokens)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to get the process's availability from a remote"
				)
			})? {
			return Ok(Some(availability));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_availability_local(
		&self,
		id: &tg::process::Id,
		token: Option<&tg::authorization::Token>,
	) -> tg::Result<Option<tg::process::Availability>> {
		let Some(storage) = self.server.try_get_process_storage_local(id).await? else {
			return Ok(None);
		};
		self.compute_process_availability(id, storage, token).await
	}

	pub(crate) async fn compute_process_availability(
		&self,
		id: &tg::process::Id,
		storage: tangram_index::process::Storage,
		token: Option<&tg::authorization::Token>,
	) -> tg::Result<Option<tg::process::Availability>> {
		let resource = tg::Referent::with_node_and_token(id.clone(), token.cloned());
		let requested = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::all(),
		);
		let Some(permissions) = self.authorize(resource, requested).await? else {
			return Ok(None);
		};
		Ok(Self::compute_process_availability_with_permissions(
			&storage,
			permissions,
		))
	}

	pub(crate) fn compute_process_availability_with_permissions(
		storage: &tangram_index::process::Storage,
		permissions: tg::authorization::permission::Set,
	) -> Option<tg::process::Availability> {
		let mut output = tg::process::Availability::default();
		let mut permitted = false;

		if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		)) {
			permitted = true;
		}

		if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Subtree,
		)) {
			output.subtree = storage.subtree;
			permitted = true;
		}

		if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeCommand,
		)) {
			output.node_command = storage.node_command;
			output.subtree_command = storage.subtree_command;
			permitted = true;
		} else if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeCommand,
		)) {
			output.node_command = storage.node_command;
			permitted = true;
		}

		if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeError,
		)) {
			output.node_error = storage.node_error;
			output.subtree_error = storage.subtree_error;
			permitted = true;
		} else if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeError,
		)) {
			output.node_error = storage.node_error;
			permitted = true;
		}

		if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeLog,
		)) {
			output.node_log = storage.node_log;
			output.subtree_log = storage.subtree_log;
			permitted = true;
		} else if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeLog,
		)) {
			output.node_log = storage.node_log;
			permitted = true;
		}

		if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeOutput,
		)) {
			output.node_output = storage.node_output;
			output.subtree_output = storage.subtree_output;
			permitted = true;
		} else if permissions.contains(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeOutput,
		)) {
			output.node_output = storage.node_output;
			permitted = true;
		}

		permitted.then_some(output)
	}

	async fn try_get_process_availability_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::Availability>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_availability_region(id, region, tokens))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(availability)) => {
					result = Ok(Some(availability));
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

	async fn try_get_process_availability_region(
		&self,
		id: &tg::process::Id,
		region: &str,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::Availability>> {
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::availability::Arg {
			location: Some(location.clone().into()),
			tokens: tokens.for_location(&location),
		};
		client.try_get_process_availability(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the process's availability"),
		)
	}

	async fn try_get_process_availability_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::Availability>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_availability_remote(id, remote, tokens))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(availability)) => {
					result = Ok(Some(availability));
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

	async fn try_get_process_availability_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::Availability>> {
		let client = self
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::process::availability::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens: tokens.for_location(&location),
		};
		client.try_get_process_availability(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the process's availability"),
		)
	}

	pub(crate) async fn try_get_process_availability_request(
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

		// Get the process's availability.
		let Some(output) = self.try_get_process_availability(&id, arg).await? else {
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
