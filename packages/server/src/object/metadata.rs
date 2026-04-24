use {
	crate::{Context, Server},
	futures::{StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

impl Server {
	pub async fn try_get_object_metadata_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(metadata) = self
					.try_get_object_metadata_local(id)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the object metadata"))?
			{
				return Ok(Some(metadata));
			}

			if let Some(metadata) = self
				.try_get_object_metadata_regions(id, &local.regions)
				.await
				.map_err(|source| {
					tg::error!(
						!source,
						"failed to get the object metadata from another region"
					)
				})? {
				return Ok(Some(metadata));
			}
		}

		if let Some(metadata) = self
			.try_get_object_metadata_remotes(id, &locations.remotes)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to get the object metadata from a remote")
			})? {
			return Ok(Some(metadata));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_object_metadata_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		Ok(self
			.index
			.try_get_object(id)
			.await?
			.map(|object| object.metadata))
	}

	pub(crate) async fn try_get_object_metadata_batch_local(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		Ok(self
			.index
			.try_get_objects(ids)
			.await?
			.into_iter()
			.map(|object| object.map(|object| object.metadata))
			.collect())
	}

	async fn try_get_object_metadata_regions(
		&self,
		id: &tg::object::Id,
		regions: &[String],
	) -> tg::Result<Option<tg::object::Metadata>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_object_metadata_region(id, region))
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

	async fn try_get_object_metadata_region(
		&self,
		id: &tg::object::Id,
		region: &str,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::object::metadata::Arg {
			location: Some(location.into()),
		};
		let Some(metadata) = client.try_get_object_metadata(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the object metadata"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	async fn try_get_object_metadata_remotes(
		&self,
		id: &tg::object::Id,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<tg::object::Metadata>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_object_metadata_remote(id, remote))
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

	async fn try_get_object_metadata_remote(
		&self,
		id: &tg::object::Id,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::object::metadata::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let Some(metadata) = client
			.try_get_object_metadata(id, arg)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					remote = %remote.name,
					"failed to get the object metadata"
				)
			})?
		else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	pub(crate) async fn handle_get_object_metadata_request(
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

		// Parse the object id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the object id"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the object metadata.
		let Some(output) = self
			.try_get_object_metadata_with_context(context, &id, arg)
			.await?
		else {
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
