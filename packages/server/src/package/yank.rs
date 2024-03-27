use crate::{
	util::http::{ok, Incoming, Outgoing},
	Http, Server,
};
use http_body_util::BodyExt;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database, Query};

impl Server {
	pub async fn get_package_yanked(&self, package: &tg::Directory) -> tg::Result<bool> {
		let id = package.id(self).await?.clone();
		if let Some(remote) = self.inner.remotes.first() {
			let dependency = tg::Dependency::with_id(id);
			let arg = tg::package::GetArg {
				yanked: true,
				..Default::default()
			};
			let output = remote
				.try_get_package(&dependency, arg)
				.await
				.map_err(|source| tg::error!(!source, %dependency, "failed to get yanked status"))?
				.ok_or_else(|| tg::error!(%dependency, "expected a package"))?;
			let yanked = output
				.yanked
				.ok_or_else(|| tg::error!("expected a `yanked` field"))?;
			return Ok(yanked);
		}

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		#[derive(serde::Deserialize)]
		struct Row {
			yanked: bool,
		}

		let p = connection.p();
		let statement = formatdoc!(
			"
                select yanked from package_versions
                where id = {p}1
            "
		);
		let params = db::params![id];
		let row = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
		Ok(row.yanked)
	}

	pub async fn yank_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> tg::Result<()> {
		if let Some(remote) = self.inner.remotes.first() {
			self.push_object(&id.clone().into()).await?;
			remote.yank_package(user, id).await?;
			return Ok(());
		}

		let yanked = true;

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the package versions table
		let p = connection.p();
		let statement = formatdoc!(
			"
                update package_versions
                set yanked = {p}1
                where (id = {p}2)
			"
		);
		let params = db::params![yanked, id];

		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(())
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_yank_package_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let package_id = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "invalid request"))?;

		// Publish the package.
		self.inner
			.tg
			.yank_package(user.as_ref(), &package_id)
			.await?;

		Ok(ok())
	}
}
