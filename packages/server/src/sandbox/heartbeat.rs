use {
	crate::{Context, Server},
	futures::{StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Server {
	pub(crate) async fn try_heartbeat_sandbox_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_heartbeat_sandbox_local(id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to heartbeat the sandbox"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_heartbeat_sandbox_regions(id, &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to heartbeat the sandbox in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_heartbeat_sandbox_remotes(id, &locations.remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to heartbeat the sandbox in a remote"),
			)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_heartbeat_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		let connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update sandboxes
				set heartbeat_at = case when status = 'started' then {p}1 else heartbeat_at end
				where id = {p}2;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, id.to_string()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n == 0 {
			return Ok(None);
		}
		drop(connection);
		let status = self.get_sandbox_status_local(id).await?;
		Ok(Some(tg::sandbox::heartbeat::Output { status }))
	}

	async fn try_heartbeat_sandbox_regions(
		&self,
		id: &tg::sandbox::Id,
		regions: &[String],
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_heartbeat_sandbox_region(id, region))
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

	async fn try_heartbeat_sandbox_region(
		&self,
		id: &tg::sandbox::Id,
		region: &str,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::heartbeat::Arg {
			location: Some(location.into()),
		};
		let Some(output) = client.try_heartbeat_sandbox(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to heartbeat the sandbox"),
		)?
		else {
			return Ok(None);
		};

		Ok(Some(output))
	}

	async fn try_heartbeat_sandbox_remotes(
		&self,
		id: &tg::sandbox::Id,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_heartbeat_sandbox_remote(id, remote))
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

	async fn try_heartbeat_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::heartbeat::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let Some(output) = client.try_heartbeat_sandbox(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to heartbeat the sandbox"),
		)?
		else {
			return Ok(None);
		};

		Ok(Some(output))
	}

	pub(crate) async fn handle_heartbeat_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let Some(output) = self
			.try_heartbeat_sandbox_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to heartbeat the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
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
		Ok(response.body(body).unwrap())
	}
}
