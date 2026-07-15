use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	indoc::formatdoc,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
};

impl Session {
	pub(crate) async fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_sandbox_local(id)
					.boxed()
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_get_sandbox_regions(id, &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the sandbox from another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_get_sandbox_remotes(id, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox from a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let permission =
			tg::grant::Permission::Sandbox(tg::grant::permission::sandbox::Permission::Read);
		let authorize_future = async {
			let authorized = self.authorize(id.clone(), permission).await?;
			Ok::<_, tg::Error>(
				authorized.is_some_and(|permissions| permissions.contains(permission)),
			)
		};
		let exists_future = self.exists(id.clone(), permission);
		let get_future = self.get_sandbox_local(id);
		let exists_future = pin!(exists_future);
		let get_future = pin!(get_future);
		let get_or_exists_future = future::select(get_future, exists_future);
		let authorize_future = pin!(authorize_future);
		let get_or_exists_future = pin!(get_or_exists_future);
		let output = match future::select(authorize_future, get_or_exists_future).await {
			future::Either::Left((authorized, get_or_exists_future)) => {
				if !authorized? {
					return Ok(None);
				}
				match get_or_exists_future.await {
					future::Either::Left((output, _)) => Some(output?),
					future::Either::Right((exists, get_future)) => {
						if exists? {
							Some(get_future.await?)
						} else {
							None
						}
					},
				}
			},
			future::Either::Right((get_or_exists, authorize_future)) => match get_or_exists {
				future::Either::Left((output, _)) => {
					let output = output?;
					authorize_future.await?.then_some(output)
				},
				future::Either::Right((exists, get_future)) => {
					if !exists? || !authorize_future.await? {
						None
					} else {
						Some(get_future.await?)
					}
				},
			},
		};
		Ok(output)
	}

	async fn get_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<tg::sandbox::get::Output> {
		if let Some(data) = self.server.runner.state.try_get_sandbox(id) {
			return Ok(data);
		}

		let control_future = self.get_sandbox_from_control(id);
		let process_store_future = self.try_get_sandbox_from_process_store(id);
		let output = match future::select(pin!(control_future), pin!(process_store_future)).await {
			future::Either::Left((result, _)) => result?,
			future::Either::Right((result, control_future)) => match result? {
				Some(output) => output,
				None => control_future.await?,
			},
		};

		Ok(output)
	}

	pub(crate) async fn get_sandbox_from_control(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<tg::sandbox::get::Output> {
		let request = tg::sandbox::control::ServerRequestArg::Get(
			tg::sandbox::control::GetServerRequestArg {},
		);
		let retry = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		};
		let options = crate::control::Options {
			retry,
			timeout: std::time::Duration::from_secs(10),
		};
		let response = self
			.send_sandbox_control_request(id, request, options)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the get sandbox control request"),
			)?
			.map_err(|error| tg::error!(!error, %id, "the get sandbox control request failed"))?;
		let response = response
			.try_unwrap_get()
			.map_err(|_| tg::error!("expected a get response"))?;
		let output = response.data;
		Ok(output)
	}

	async fn try_get_sandbox_from_process_store(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			cpu: Option<i64>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			creator: Option<tg::Principal>,
			hostname: Option<String>,
			#[tangram_database(as = "Option<db::value::Json<tg::sandbox::Isolation>>")]
			isolation: Option<tg::sandbox::Isolation>,
			memory: Option<i64>,
			#[tangram_database(as = "Option<db::value::Json<Vec<tg::sandbox::Mount>>>")]
			mounts: Option<Vec<tg::sandbox::Mount>>,
			#[tangram_database(as = "Option<db::value::Json<tg::sandbox::Network>>")]
			network: Option<tg::sandbox::Network>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			owner: Option<tg::Principal>,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::sandbox::Status,
			#[tangram_database(as = "Option<db::value::DurationSeconds>")]
			ttl: Option<std::time::Duration>,
		}
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r"
				select
					cpu,
					creator,
					hostname,
					isolation,
					memory,
					mounts,
					network,
					owner,
					status,
					ttl
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let output = row
			.map(|row| {
				Ok::<_, tg::Error>(tg::sandbox::get::Output {
					cpu: row
						.cpu
						.map(u64::try_from)
						.transpose()
						.map_err(|error| tg::error!(!error, "invalid sandbox cpu"))?,
					creator: row.creator,
					hostname: row.hostname,
					id: id.clone(),
					isolation: row.isolation,
					location: Some(self.server.config().region.clone().map_or_else(
						|| tg::Location::Local(tg::location::Local::default()),
						|region| {
							tg::Location::Local(tg::location::Local {
								region: Some(region),
							})
						},
					)),
					memory: row
						.memory
						.map(u64::try_from)
						.transpose()
						.map_err(|error| tg::error!(!error, "invalid sandbox memory"))?,
					mounts: row.mounts.unwrap_or_default(),
					network: row.network,
					owner: Some(row.owner.unwrap_or(tg::Principal::Root)),
					status: row.status,
					ttl: row.ttl,
				})
			})
			.transpose()?;
		Ok(output)
	}

	async fn try_get_sandbox_regions(
		&self,
		id: &tg::sandbox::Id,
		regions: &[String],
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_sandbox_region(id, region))
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

	async fn try_get_sandbox_region(
		&self,
		id: &tg::sandbox::Id,
		region: &str,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::get::Arg {
			location: Some(location.clone().into()),
		};
		let Some(mut output) = client
			.try_get_sandbox(id, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to get the sandbox"))?
		else {
			return Ok(None);
		};
		output.location = Some(location);
		Ok(Some(output))
	}

	async fn try_get_sandbox_remotes(
		&self,
		id: &tg::sandbox::Id,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_sandbox_remote(id, remote))
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

	async fn try_get_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::get::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let Some(mut output) = client.try_get_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the sandbox"),
		)?
		else {
			return Ok(None);
		};
		output.location = Some(tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		}));
		Ok(Some(output))
	}

	pub(crate) async fn try_get_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let Some(output) = self.try_get_sandbox(&id, arg).boxed().await? else {
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
