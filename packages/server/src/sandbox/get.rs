use {
	crate::{Context, Server},
	futures::{StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
};

impl Server {
	pub(crate) async fn try_get_sandbox_with_context(
		&self,
		_context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let locations = self
			.locations_with_regions(arg.locations)
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_sandbox_local(id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_get_sandbox_from_regions(id, &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to get the sandbox from another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_get_sandbox_from_remotes(id, &locations.remotes)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			cpu: Option<i64>,
			hostname: Option<String>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			isolation: Option<tg::sandbox::Isolation>,
			memory: Option<i64>,
			#[tangram_database(as = "Option<db::value::Json<Vec<tg::sandbox::Mount>>>")]
			mounts: Option<Vec<tg::sandbox::Mount>>,
			network: bool,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::sandbox::Status,
			ttl: i64,
			user: Option<String>,
		}
		let connection = self
			.sandbox_store
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					cpu,
					hostname,
					isolation,
					memory,
					mounts,
					network,
					status,
					ttl,
					\"user\" as user
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let row = row
			.map(|row| {
				Ok::<_, tg::Error>(tg::sandbox::get::Output {
					cpu: row
						.cpu
						.map(u64::try_from)
						.transpose()
						.map_err(|source| tg::error!(!source, "invalid sandbox cpu"))?,
					id: id.clone(),
					location: Some(self.config().region.clone().map_or_else(
						|| tg::location::Location::Local(tg::location::Local::default()),
						|region| {
							tg::location::Location::Local(tg::location::Local {
								regions: Some(vec![region]),
							})
						},
					)),
					hostname: row.hostname,
					isolation: row.isolation,
					memory: row
						.memory
						.map(u64::try_from)
						.transpose()
						.map_err(|source| tg::error!(!source, "invalid sandbox memory"))?,
					mounts: row.mounts.unwrap_or_default(),
					network: row.network,
					status: row.status,
					ttl: u64::try_from(row.ttl).unwrap(),
					user: row.user,
				})
			})
			.transpose()?;
		Ok(row)
	}

	async fn try_get_sandbox_from_regions(
		&self,
		id: &tg::sandbox::Id,
		regions: &[String],
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_sandbox_from_region(id, region))
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

	async fn try_get_sandbox_from_region(
		&self,
		id: &tg::sandbox::Id,
		region: &str,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let arg = tg::sandbox::get::Arg {
			locations: tg::location::Locations {
				local: Some(tg::Either::Right(tg::location::Local {
					regions: Some(vec![region.to_owned()]),
				})),
				remotes: Some(tg::Either::Left(false)),
			},
		};
		let Some(output) = client
			.try_get_sandbox(id, arg)
			.await
			.map_err(|source| tg::error!(!source, region = %region, "failed to get the sandbox"))?
		else {
			return Ok(None);
		};
		let mut output = output;
		output.location = Some(tg::location::Location::Local(tg::location::Local {
			regions: Some(vec![region.to_owned()]),
		}));
		Ok(Some(output))
	}

	async fn try_get_sandbox_from_remotes(
		&self,
		id: &tg::sandbox::Id,
		remotes: &[tg::location::Remote],
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_sandbox_from_remote(id, remote))
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

	async fn try_get_sandbox_from_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: &tg::location::Remote,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let client = self
			.get_remote_client(remote.remote.clone())
			.await
			.map_err(
				|source| tg::error!(!source, %id, remote = %remote.remote, "failed to get the remote client"),
			)?;
		let arg = tg::sandbox::get::Arg {
			locations: tg::location::Locations {
				local: match &remote.regions {
					Some(regions) => Some(tg::Either::Right(tg::location::Local {
						regions: Some(regions.clone()),
					})),
					None => Some(tg::Either::Left(true)),
				},
				remotes: Some(tg::Either::Left(false)),
			},
		};
		let Some(output) = client.try_get_sandbox(id, arg).await.map_err(
			|source| tg::error!(!source, %id, remote = %remote.remote, "failed to get the sandbox"),
		)?
		else {
			return Ok(None);
		};
		let mut output = output;
		output.location = Some(tg::location::Location::Remote(remote.clone()));
		Ok(Some(output))
	}

	pub(crate) async fn handle_get_sandbox_request(
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
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let Some(output) = self.try_get_sandbox_with_context(context, &id, arg).await? else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
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
