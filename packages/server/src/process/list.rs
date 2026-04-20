use {
	crate::{Context, Server, database::Database},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn list_processes_with_context(
		&self,
		context: &Context,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut output = tg::process::list::Output { data: Vec::new() };

		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current {
				let local_outputs = self
					.list_processes_local()
					.await
					.map_err(|source| tg::error!(!source, "failed to list local processes"))?;
				output.data.extend(local_outputs);
			}

			let region_outputs =
				self.list_processes_regions(&local.regions)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to list processes from other regions")
					})?;
			output
				.data
				.extend(region_outputs.into_iter().flat_map(|output| output.data));
		}

		let remote_outputs = self
			.list_processes_remotes(&locations.remotes)
			.await
			.map_err(|source| tg::error!(!source, "failed to list processes from remotes"))?;
		output
			.data
			.extend(remote_outputs.into_iter().flat_map(|output| output.data));

		Ok(output)
	}

	pub(crate) async fn list_processes_local(&self) -> tg::Result<Vec<tg::process::get::Output>> {
		let mut output = match &self.process_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(process_store) => self.list_processes_postgres(process_store).await,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(process_store) => self.list_processes_sqlite(process_store).await,
		}?;
		let location = Some(self.config().region.clone().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|region| {
				tg::Location::Local(tg::location::Local {
					region: Some(region),
				})
			},
		));
		for process in &mut output {
			process.location = location.clone();
		}
		Ok(output)
	}

	async fn list_processes_regions(
		&self,
		regions: &[String],
	) -> tg::Result<Vec<tg::process::list::Output>> {
		let outputs = regions
			.iter()
			.map(|region| self.list_processes_region(region))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	async fn list_processes_region(&self, region: &str) -> tg::Result<tg::process::list::Output> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::list::Arg {
			location: Some(location.clone().into()),
		};
		let mut output = client
			.list_processes(arg)
			.await
			.map_err(|source| tg::error!(!source, region = %region, "failed to list processes"))?;
		for process in &mut output.data {
			process.location = Some(location.clone());
		}
		Ok(output)
	}

	async fn list_processes_remotes(
		&self,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Vec<tg::process::list::Output>> {
		let outputs = remotes
			.iter()
			.map(|remote| self.list_processes_remote(remote))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	async fn list_processes_remote(
		&self,
		remote: &crate::location::Remote,
	) -> tg::Result<tg::process::list::Output> {
		let client = self
			.get_remote_client(remote.remote.clone())
			.await
			.map_err(
				|source| tg::error!(!source, remote = %remote.remote, "failed to get the remote client"),
			)?;
		let arg = tg::process::list::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
		};
		let mut output = client.list_processes(arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.remote, "failed to list processes"),
		)?;
		for process in &mut output.data {
			process.location = Some(tg::Location::Remote(tg::location::Remote {
				name: remote.remote.clone(),
				region: None,
			}));
		}
		Ok(output)
	}

	pub(crate) async fn handle_list_processes_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// List the processes.
		let output = self
			.list_processes_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the processes"))?;
		let output = output.data;

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
