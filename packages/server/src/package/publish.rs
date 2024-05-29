use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn publish_package(
		&self,
		id: &tg::artifact::Id,
		arg: tg::package::publish::Arg,
	) -> tg::Result<()> {
		let remote = arg.remote.as_ref().or(self.options.registry.as_ref());
		match remote {
			None => {
				self.publish_package_local(id, arg).await?;
				Ok(())
			},
			Some(remote) => {
				let remote = self
					.remotes
					.get(remote)
					.ok_or_else(|| tg::error!("the remote does not exist"))?
					.clone();
				remote.publish_package(id, arg).await?;
				Ok(())
			},
		}
	}

	pub async fn publish_package_local(
		&self,
		id: &tg::artifact::Id,
		_arg: tg::package::publish::Arg,
	) -> tg::Result<()> {
		// Get the package.
		let package = tg::Artifact::with_id(id.clone())
			.try_unwrap_directory()
			.ok()
			.ok_or_else(|| tg::error!("expected a directory"))?;

		// Get the metadata.
		let metadata = tg::package::get_metadata(self, &package.clone().into()).await?;

		// Get the package name and version.
		let name = metadata
			.name
			.as_ref()
			.ok_or_else(|| tg::error!(%id, "the package must have a name"))?
			.as_str();
		let version = metadata
			.version
			.as_ref()
			.ok_or_else(|| tg::error!(%id, "the package must have a version"))?
			.as_str();

		// Check if the package has been published already.
		let mut published_versions =  self.try_get_package_versions_local(&tg::Dependency::with_name(name.to_owned())).await?.into_iter().flatten();
		if published_versions.find(|(published, _)| published == version).is_some() {
			return Err(tg::error!(%name, %version, "package already exists"));
		}

		// Get the published at timestamp.
		let published_at = time::OffsetDateTime::now_utc();

		// Initialize yanked to false.
		let yanked = false;

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Create the package version.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into package_versions (name, version, artifact, published_at, yanked)
				values ({p}1, {p}2, {p}3, {p}4, {p}5);
			"
		);
		let params = db::params![
			name,
			version,
			id,
			published_at.format(&Rfc3339).unwrap(),
			yanked
		];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_publish_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		handle.publish_package(&id, arg).await?;
		let response = http::Response::builder().ok().empty().unwrap();
		Ok(response)
	}
}
