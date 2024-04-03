use crate::{
	util::http::{ok, Incoming, Outgoing},
	Http, Server,
};
use http_body_util::BodyExt;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_error::{error, Result};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn publish_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> Result<()> {
		if let Some(remote) = self.inner.remotes.first() {
			self.push_object(&id.clone().into()).await?;
			remote.publish_package(user, id).await?;
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
			.ok_or_else(|| error!(%id, "the package must have a name"))?
			.as_str();
		let version = metadata
			.version
			.as_ref()
			.ok_or_else(|| error!(%id, "the package must have a version"))?
			.as_str();

		// Get the published at timestamp.
		let published_at = time::OffsetDateTime::now_utc();

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Create the package version.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into package_versions (name, version, id, published_at)
				values ({p}1, {p}2, {p}3, {p}4);
			"
		);
		let params = db::params![name, version, id, published_at.format(&Rfc3339).unwrap()];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
	}
}

impl Http {
	pub async fn handle_publish_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to read the body"))?
			.to_bytes();
		let package_id =
			serde_json::from_slice(&bytes).map_err(|source| error!(!source, "invalid request"))?;

		// Publish the package.
		self.inner
			.tg
			.publish_package(user.as_ref(), &package_id)
			.await?;

		Ok(ok())
	}
}
