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
	pub async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(metadata) = self
					.try_get_process_metadata_local(id, arg.token.as_ref())
					.await
					.map_err(|error| tg::error!(!error, "failed to get the process metadata"))?
			{
				return Ok(Some(metadata));
			}

			if let Some(metadata) = self
				.try_get_process_metadata_regions(id, &local.regions, arg.token.as_ref())
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						"failed to get the process metadata from another region"
					)
				})? {
				return Ok(Some(metadata));
			}
		}

		if let Some(metadata) = self
			.try_get_process_metadata_remotes(id, &locations.remotes, arg.token.as_ref())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to get the process metadata from a remote")
			})? {
			return Ok(Some(metadata));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_metadata_local(
		&self,
		id: &tg::process::Id,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let Some(metadata) = self.server.try_get_process_metadata_local(id).await? else {
			return Ok(None);
		};
		self.mask_process_metadata(id, metadata, token).await
	}

	pub(crate) async fn mask_process_metadata(
		&self,
		id: &tg::process::Id,
		metadata: tg::process::Metadata,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let resource = tg::Referent::with_item_and_token(id.clone(), token.cloned());
		let requested =
			tg::grant::permission::Set::Process(tg::grant::permission::process::Set::all());
		let Some(permissions) = self.authorize(resource, requested).await? else {
			return Ok(None);
		};
		Ok(Self::mask_process_metadata_with_permissions(
			&metadata,
			permissions,
		))
	}

	pub(crate) fn mask_process_metadata_with_permissions(
		metadata: &tg::process::Metadata,
		permissions: tg::grant::permission::Set,
	) -> Option<tg::process::Metadata> {
		let mut output = tg::process::Metadata::default();
		let mut authorized = false;

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::Node,
		)) {
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::Subtree,
		)) {
			output.subtree.count = metadata.subtree.count;
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::SubtreeCommand,
		)) {
			output.node.command = metadata.node.command.clone();
			output.subtree.command = metadata.subtree.command.clone();
			authorized = true;
		} else if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::NodeCommand,
		)) {
			output.node.command = metadata.node.command.clone();
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::SubtreeError,
		)) {
			output.node.error = metadata.node.error.clone();
			output.subtree.error = metadata.subtree.error.clone();
			authorized = true;
		} else if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::NodeError,
		)) {
			output.node.error = metadata.node.error.clone();
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::SubtreeLog,
		)) {
			output.node.log = metadata.node.log.clone();
			output.subtree.log = metadata.subtree.log.clone();
			authorized = true;
		} else if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::NodeLog,
		)) {
			output.node.log = metadata.node.log.clone();
			authorized = true;
		}

		if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::SubtreeOutput,
		)) {
			output.node.output = metadata.node.output.clone();
			output.subtree.output = metadata.subtree.output.clone();
			authorized = true;
		} else if permissions.contains(tg::grant::Permission::Process(
			tg::grant::permission::process::Permission::NodeOutput,
		)) {
			output.node.output = metadata.node.output.clone();
			authorized = true;
		}

		if !authorized {
			return None;
		}

		Some(output)
	}

	async fn try_get_process_metadata_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_metadata_region(id, region, token))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(metadata)) => {
					result = Ok(Some(metadata));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(metadata) = result? else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	async fn try_get_process_metadata_region(
		&self,
		id: &tg::process::Id,
		region: &str,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::metadata::Arg {
			location: Some(location.into()),
			token: token.cloned(),
		};
		let Some(metadata) = client.try_get_process_metadata(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the process metadata"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	async fn try_get_process_metadata_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_metadata_remote(id, remote, token))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(metadata)) => {
					result = Ok(Some(metadata));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(metadata) = result? else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	async fn try_get_process_metadata_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::process::metadata::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			token: token.cloned(),
		};
		let Some(metadata) = client.try_get_process_metadata(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the process metadata"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	pub(crate) async fn try_get_process_metadata_request(
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

		// Get the process metadata.
		let Some(output) = self.try_get_process_metadata(&id, arg).await? else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
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
	pub(crate) async fn try_get_process_metadata_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let Some(_) = self.try_get_process_local(id, false).await? else {
			return Ok(None);
		};
		Ok(self
			.index
			.try_get_process(id)
			.await?
			.map(|process| process.metadata))
	}
}
