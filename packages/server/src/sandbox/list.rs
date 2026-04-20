use {
	crate::{Context, Server},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub(crate) async fn list_sandboxes_with_context(
		&self,
		context: &Context,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut output = tg::sandbox::list::Output { data: Vec::new() };

		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current {
				output.data.extend(self.list_sandboxes_local().await?);
			}

			let region_outputs = self.list_sandboxes_regions(&local.regions).await?;
			output
				.data
				.extend(region_outputs.into_iter().flat_map(|output| output.data));
		}

		let remote_outputs = self.list_sandboxes_remotes(&locations.remotes).await?;
		output
			.data
			.extend(remote_outputs.into_iter().flat_map(|output| output.data));

		Ok(output)
	}

	async fn list_sandboxes_local(&self) -> tg::Result<Vec<tg::sandbox::list::Item>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::sandbox::Id,
			cpu: Option<i64>,
			hostname: Option<String>,
			memory: Option<i64>,
			#[tangram_database(as = "Option<db::value::Json<Vec<tg::sandbox::Mount>>>")]
			mounts: Option<Vec<tg::sandbox::Mount>>,
			network: bool,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::sandbox::Status,
			ttl: i64,
			user: Option<String>,
		}
		let connection =
			self.process_store.connection().await.map_err(|source| {
				tg::error!(!source, "failed to get a process store connection")
			})?;
		let statement = formatdoc!(
			"
				select id, cpu, hostname, memory, mounts, network, status, ttl, \"user\" as user
				from sandboxes
				where status != 'finished'
				order by created_at;
			"
		);
		let params = db::params![];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| {
				Ok::<_, tg::Error>(tg::sandbox::list::Item {
					id: row.id,
					cpu: row
						.cpu
						.map(u64::try_from)
						.transpose()
						.map_err(|source| tg::error!(!source, "invalid sandbox cpu"))?,
					hostname: row.hostname,
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
			.collect::<tg::Result<_>>()?;
		Ok(data)
	}

	async fn list_sandboxes_regions(
		&self,
		regions: &[String],
	) -> tg::Result<Vec<tg::sandbox::list::Output>> {
		let outputs = regions
			.iter()
			.map(|region| self.list_sandboxes_region(region))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	async fn list_sandboxes_region(&self, region: &str) -> tg::Result<tg::sandbox::list::Output> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::list::Arg {
			location: Some(location.into()),
		};
		let output = client.list_sandboxes(arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to list the sandboxes"),
		)?;
		Ok(output)
	}

	async fn list_sandboxes_remotes(
		&self,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Vec<tg::sandbox::list::Output>> {
		let outputs = remotes
			.iter()
			.map(|remote| self.list_sandboxes_remote(remote))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	async fn list_sandboxes_remote(
		&self,
		remote: &crate::location::Remote,
	) -> tg::Result<tg::sandbox::list::Output> {
		let client = self
			.get_remote_client(remote.remote.clone())
			.await
			.map_err(
				|source| tg::error!(!source, remote = %remote.remote, "failed to get the remote client"),
			)?;
		let arg = tg::sandbox::list::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let output = client.list_sandboxes(arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.remote, "failed to list the sandboxes"),
		)?;
		Ok(output)
	}

	pub(crate) async fn handle_list_sandboxes_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		let output = self
			.list_sandboxes_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the sandboxes"))?;

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
