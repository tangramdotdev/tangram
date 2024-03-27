use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database, Query};

impl Server {
	/// Update the path of a package.
	pub(super) async fn set_package_path(
		&self,
		path: &tg::Path,
		package: &tg::Directory,
	) -> tg::Result<()> {
		let id = package.id(self).await?;

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let p = connection.p();
		let statement = formatdoc!(
			"
                insert into package_paths (path, package)
                values ({p}1, {p}2)
                on conflict (path) do update set
					path = {p}1,
					package = {p}2;
            "
		);
		let params = db::params![path, id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}

	/// Get the path for a package if it exists.
	pub async fn try_get_package_path(
		&self,
		package: &tg::Directory,
	) -> tg::Result<Option<tg::Path>> {
		let id = package.id(self).await?.clone();

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		#[derive(serde::Deserialize)]
		struct Row {
			path: tg::Path,
		}

		let p = connection.p();
		let statement = formatdoc!(
			"
                select path from package_paths
                where package = {p}1
            "
		);
		let params = db::params![id];
		let path = connection
			.query_optional_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?
			.map(|row| row.path);
		Ok(path)
	}
}
