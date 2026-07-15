use {
	crate::Session,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
		}
		let creator = self.context.principal.clone();

		let mut output = tg::sandbox::list::Output { data: Vec::new() };

		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current {
				output.data.extend(
					self.list_sandboxes_local(Some(&creator), arg.owner.as_ref())
						.await?,
				);
			}

			let region_outputs = self
				.list_sandboxes_regions(&local.regions, arg.owner.as_ref())
				.await?;
			output
				.data
				.extend(region_outputs.into_iter().flat_map(|output| output.data));
		}

		let remote_outputs = self
			.list_sandboxes_remotes(&locations.remotes, arg.owner.as_ref())
			.await?;
		output
			.data
			.extend(remote_outputs.into_iter().flat_map(|output| output.data));

		Ok(output)
	}

	pub(crate) async fn list_sandboxes_local(
		&self,
		creator: Option<&tg::Principal>,
		owner: Option<&tg::Principal>,
	) -> tg::Result<Vec<tg::sandbox::list::Item>> {
		let mut sandboxes = if let Some(owner) = owner {
			self.server
				.index
				.list_sandboxes_for_owner(owner)
				.await
				.map_err(
					|error| tg::error!(!error, %owner, "failed to list the sandboxes for the owner"),
				)?
		} else if let Some(creator) = creator {
			let creator_sandboxes = self
				.server
				.index
				.list_sandboxes_for_creator(creator)
				.await
				.map_err(
					|error| tg::error!(!error, %creator, "failed to list the sandboxes for the creator"),
				)?;
			let mut sandboxes = creator_sandboxes.into_iter().collect::<BTreeMap<_, _>>();
			let principals = self
				.server
				.index
				.get_requester_principals(creator)
				.await
				.map_err(
					|error| tg::error!(!error, %creator, "failed to get the requester principals"),
				)?;
			for owner in principals
				.into_iter()
				.filter_map(|principal| principal.try_to_principal().ok())
			{
				let owner_sandboxes = self
					.server
					.index
					.list_sandboxes_for_owner(&owner)
					.await
					.map_err(
						|error| tg::error!(!error, %owner, "failed to list the sandboxes for the owner"),
					)?;
				sandboxes.extend(owner_sandboxes);
			}
			sandboxes.into_iter().collect()
		} else {
			self.server
				.index
				.list_sandboxes()
				.await
				.map_err(|error| tg::error!(!error, "failed to list the sandboxes"))?
		};
		sandboxes.sort_by(|(id_a, sandbox_a), (id_b, sandbox_b)| {
			sandbox_a
				.created_at
				.cmp(&sandbox_b.created_at)
				.then_with(|| id_a.cmp(id_b))
		});
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let data = sandboxes
			.into_iter()
			.filter_map(|(id, sandbox)| {
				let data = sandbox.data?;
				if !data.status.is_started()
					|| data
						.location
						.as_ref()
						.is_some_and(|sandbox_location| sandbox_location != &location)
				{
					return None;
				}
				Some(tg::sandbox::list::Item {
					cpu: data.cpu,
					hostname: data.hostname,
					id,
					memory: data.memory,
					mounts: data.mounts,
					network: data.network,
					owner: Some(data.owner.unwrap_or(tg::Principal::Root)),
					status: data.status,
					ttl: data.ttl,
				})
			})
			.collect::<Vec<_>>();
		let permission =
			tg::grant::Permission::Sandbox(tg::grant::permission::sandbox::Permission::Read);
		let authorizations = data
			.iter()
			.map(|item| {
				(
					tg::grant::Resource::Id(tg::Id::from(item.id.clone())),
					permission.into(),
				)
			})
			.collect::<Vec<_>>();
		let authorizations = self
			.authorize_batch(authorizations)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the sandboxes"))?;
		let data = std::iter::zip(data, authorizations)
			.filter_map(|(item, permissions)| {
				permissions
					.is_some_and(|permissions| permissions.contains(permission))
					.then_some(item)
			})
			.collect();
		Ok(data)
	}

	async fn list_sandboxes_regions(
		&self,
		regions: &[String],
		owner: Option<&tg::Principal>,
	) -> tg::Result<Vec<tg::sandbox::list::Output>> {
		let outputs = regions
			.iter()
			.map(|region| self.list_sandboxes_region(region, owner))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	async fn list_sandboxes_region(
		&self,
		region: &str,
		owner: Option<&tg::Principal>,
	) -> tg::Result<tg::sandbox::list::Output> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::list::Arg {
			location: Some(location.into()),
			owner: owner.cloned(),
		};
		let output = client.list_sandboxes(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to list the sandboxes"),
		)?;
		Ok(output)
	}

	async fn list_sandboxes_remotes(
		&self,
		remotes: &[crate::location::Remote],
		owner: Option<&tg::Principal>,
	) -> tg::Result<Vec<tg::sandbox::list::Output>> {
		let outputs = remotes
			.iter()
			.map(|remote| self.list_sandboxes_remote(remote, owner))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	async fn list_sandboxes_remote(
		&self,
		remote: &crate::location::Remote,
		owner: Option<&tg::Principal>,
	) -> tg::Result<tg::sandbox::list::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::list::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			owner: owner.cloned(),
		};
		let output = client.list_sandboxes(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to list the sandboxes"),
		)?;
		Ok(output)
	}

	pub(crate) async fn list_sandboxes_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		let output = self
			.list_sandboxes(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the sandboxes"))?;

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
