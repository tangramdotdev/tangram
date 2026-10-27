use {
	crate::Component,
	futures::{StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_server_core::{Server, Session},
};

impl Component {
	pub async fn try_get_process_metadata(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		tangram_server_core::checkpoint!(session.server, "process.metadata", id = %id).await;
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(metadata) = self
					.try_get_process_metadata_local(session, id, arg.tokens.local_authorization())
					.await
					.map_err(|error| tg::error!(!error, "failed to get the process metadata"))?
			{
				return Ok(Some(metadata));
			}

			if let Some(metadata) = self
				.try_get_process_metadata_regions(session, id, &local.regions, &arg.tokens)
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
			.try_get_process_metadata_remotes(session, id, &locations.remotes, &arg.tokens)
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
		session: &Session,
		id: &tg::process::Id,
		tokens: &[tg::authorization::Token],
	) -> tg::Result<Option<tg::process::Metadata>> {
		let Some(metadata) = self
			.try_get_process_metadata_local_inner(&session.server, id)
			.await?
		else {
			return Ok(None);
		};
		self.mask_process_metadata(session, id, metadata, tokens)
			.await
	}

	pub(crate) async fn try_get_process_metadata_local_with_permissions(
		&self,
		session: &Session,
		id: &tg::process::Id,
		permissions: tg::authorization::permission::Set,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let Some(metadata) = self
			.try_get_process_metadata_local_inner(&session.server, id)
			.await?
		else {
			return Ok(None);
		};
		Ok(
			tangram_server_process_common::mask_process_metadata_with_permissions(
				&metadata,
				permissions,
			),
		)
	}

	pub(crate) async fn mask_process_metadata(
		&self,
		session: &Session,
		id: &tg::process::Id,
		metadata: tg::process::Metadata,
		tokens: &[tg::authorization::Token],
	) -> tg::Result<Option<tg::process::Metadata>> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let requested = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::all(),
		);
		let authorization = session
			.authorize(resource, requested)
			.await?
			.check_exhaustion()?;
		let permissions = authorization.permissions;
		Ok(
			tangram_server_process_common::mask_process_metadata_with_permissions(
				&metadata,
				permissions,
			),
		)
	}

	async fn try_get_process_metadata_regions(
		&self,
		session: &Session,
		id: &tg::process::Id,
		regions: &[String],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_metadata_region(session, id, region, tokens))
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
		session: &Session,
		id: &tg::process::Id,
		region: &str,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the region client"),
			)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::metadata::Arg {
			location: Some(location.clone().into()),
			tokens: tokens.for_location(&location),
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
		session: &Session,
		id: &tg::process::Id,
		remotes: &[tangram_server_core::location::Remote],
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_metadata_remote(session, id, remote, tokens))
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
		session: &Session,
		id: &tg::process::Id,
		remote: &tangram_server_core::location::Remote,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session_for_process(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::process::metadata::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens: tokens.for_location(&location),
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
		session: &Session,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the process id"))?;

		// Get the arg.
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the process metadata.
		let Some(output) = self.try_get_process_metadata(session, &id, arg).await? else {
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}

	pub(crate) async fn try_get_process_metadata_local_inner(
		&self,
		server: &Server,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let Some(process) = server.index.try_get_process(id).await? else {
			return Ok(None);
		};
		if !process.storage.contains(tg::process::storage::Set::NODE) {
			return Ok(None);
		}
		Ok(Some(process.metadata))
	}
}
