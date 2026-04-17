use {
	crate::{Context, Server},
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
	pub(crate) async fn try_finish_sandbox_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> tg::Result<Option<()>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self.location_with_regions(arg.location.as_ref())?;

		let output = match location {
			crate::location::Location::Local { region: None } => {
				self.try_finish_sandbox_local(id).await?
			},
			crate::location::Location::Local {
				region: Some(region),
			} => self.try_finish_sandbox_region(id, region).await?,
			crate::location::Location::Remote { remote, region } => {
				self.try_finish_sandbox_remote(id, remote, region).await?
			},
		};

		Ok(output)
	}

	pub(crate) async fn try_finish_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<()>> {
		let connection = self
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update sandboxes
				set
					finished_at = {p}1,
					heartbeat_at = null,
					status = 'finished'
				where id = {p}2 and status != 'finished';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, id.to_string()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		if n == 0 {
			return Ok(None);
		}
		self.publish_sandbox_status(id);
		Ok(Some(()))
	}

	async fn try_finish_sandbox_region(
		&self,
		id: &tg::sandbox::Id,
		region: String,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, region = %region, %id, "failed to get the region client"),
		)?;
		let arg = tg::sandbox::finish::Arg {
			location: Some(tg::location::Location::Local(tg::location::Local {
				regions: Some(vec![region.clone()]),
			})),
		};
		let Some(()) = client.try_finish_sandbox(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to finish the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_finish_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_client(remote.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::finish::Arg {
			location: Some(tg::location::Location::Local(tg::location::Local {
				regions: region.map(|region| vec![region]),
			})),
		};
		let Some(()) = client.try_finish_sandbox(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote, "failed to finish the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn handle_finish_sandbox_request(
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

		let Some(()) = self
			.try_finish_sandbox_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to finish the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		Ok(http::Response::builder().empty().unwrap().boxed_body())
	}
}
