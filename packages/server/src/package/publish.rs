use crate::{database::Database, postgres_params, Http, Server};
use http_body_util::BodyExt;
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_util::http::{ok, Incoming, Outgoing};

impl Server {
	pub async fn publish_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> Result<()> {
		if let Some(remote) = self.inner.remote.as_ref() {
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
			.ok_or_else(|| error!(%id, "The package must have a name."))?
			.as_str();
		let version = metadata
			.version
			.as_ref()
			.ok_or_else(|| error!(%id, "The package must have a version."))?
			.as_str();

		// Get a database connection.
		let Database::Postgres(database) = &self.inner.database else {
			return Err(error!("unimplemented"));
		};
		let connection = database.get().await?;

		// Create the package if it does not exist.
		let statement = "
			upsert into packages (name)
			values ($1);
		";
		let params = postgres_params![name];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "Failed to prepare the statement."))?;
		connection
			.execute(&statement, params)
			.await
			.map_err(|error| error!(source = error, "Failed to execute the statement."))?;

		// Create the package version.
		let statement = "
			insert into package_versions (name, version, id)
			values ($1, $2, $3);
		";
		let params = postgres_params![name, version, id.to_string()];
		let statement = connection
			.prepare_cached(statement)
			.await
			.map_err(|error| error!(source = error, "Failed to prepare the statement."))?;
		connection
			.execute(&statement, params)
			.await
			.map_err(|error| error!(source = error, "Failed to execute the statement."))?;

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
			.map_err(|error| error!(source = error, "Failed to read the body."))?
			.to_bytes();
		let package_id = serde_json::from_slice(&bytes)
			.map_err(|error| error!(source = error, "Invalid request."))?;

		// Publish the package.
		self.inner
			.tg
			.publish_package(user.as_ref(), &package_id)
			.await?;

		Ok(ok())
	}
}
