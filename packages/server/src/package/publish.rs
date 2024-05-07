use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::ResponseExt as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn publish_package(&self, id: &tg::directory::Id) -> tg::Result<()> {
		if let Some(remote) = self.remotes.first() {
			self.push_object(&id.clone().into()).await?;
			remote.publish_package(id).await?;
			return Ok(());
		}

		// Get the package.
		let package = tg::Directory::with_id(id.clone());

		// Get the metadata.
		let metadata = tg::package::get_metadata(self, &package).await?;

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

		// Create the package if it does not exist.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into packages (name)
				values ({p}1)
				on conflict (name) do nothing;
			"
		);
		let params = db::params![name];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

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
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Publish the package.
		handle.publish_package(&id).await?;

		// Create the response.
		let response = http::Response::ok();

		Ok(response)
	}
}
