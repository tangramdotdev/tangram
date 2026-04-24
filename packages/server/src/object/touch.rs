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
	pub async fn try_touch_object_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<Option<()>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_touch_object_local(id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_touch_object_regions(id, &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to touch the object in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_touch_object_remotes(id, &locations.remotes)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the object in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_touch_object_local(&self, id: &tg::object::Id) -> tg::Result<Option<()>> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let Some(_) = self
			.index
			.touch_object(id, touched_at)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_touch_object_regions(
		&self,
		id: &tg::object::Id,
		regions: &[String],
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_touch_object_region(id, region))
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

	async fn try_touch_object_region(
		&self,
		id: &tg::object::Id,
		region: &str,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, %id, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::object::touch::Arg {
			location: Some(location.into()),
		};
		let Some(()) = client.try_touch_object(id, arg).await.map_err(
			|source| tg::error!(!source, %id, region = %region, "failed to touch the object"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_touch_object_remotes(
		&self,
		id: &tg::object::Id,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_touch_object_remote(id, remote))
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

	async fn try_touch_object_remote(
		&self,
		id: &tg::object::Id,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, %id, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::object::touch::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let Some(()) = client.try_touch_object(id, arg).await.map_err(
			|source| tg::error!(!source, %id, remote = %remote.name, "failed to touch the object"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn handle_touch_object_request(
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
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Touch the object.
		let Some(()) = self
			.try_touch_object_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?
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
