use crate::Server;
use futures::{FutureExt as _, future};
use indoc::{formatdoc, indoc};
use rusqlite as sqlite;
use tangram_client::{self as tg, prelude::*};
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};

impl Server {
	pub async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		if let Some(metadata) = self.try_get_process_metadata_local(id).await? {
			Ok(Some(metadata))
		} else if let Some(metadata) = self.try_get_process_metadata_remote(id).await? {
			Ok(Some(metadata))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_process_metadata_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_metadata_postgres(database, id).await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_metadata_sqlite(database, id).await
			},
		}
	}

	#[cfg(feature = "postgres")]
	async fn try_get_process_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		// Get a database connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the process metadata.
		let p = connection.p();
		#[derive(serde::Deserialize)]
		struct Row {
			children_count: Option<u64>,
			commands_count: Option<u64>,
			commands_depth: Option<u64>,
			commands_weight: Option<u64>,
			outputs_count: Option<u64>,
			outputs_depth: Option<u64>,
			outputs_weight: Option<u64>,
		}
		let statement = formatdoc!(
			"
				select
					children_count,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_count,
					outputs_depth,
					outputs_weight
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0)
			.map(|row| {
				let children = tg::process::metadata::Children {
					count: row.children_count,
				};
				let commands = tg::object::Metadata {
					count: row.commands_count,
					depth: row.commands_depth,
					weight: row.commands_weight,
				};
				let outputs = tg::object::Metadata {
					count: row.outputs_count,
					depth: row.outputs_depth,
					weight: row.outputs_weight,
				};
				tg::process::Metadata {
					children,
					commands,
					outputs,
				}
			});

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	async fn try_get_process_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		// Get a database connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the process metadata.
		let p = connection.p();
		#[derive(serde::Deserialize)]
		struct Row {
			children_count: Option<u64>,
			commands_count: Option<u64>,
			commands_depth: Option<u64>,
			commands_weight: Option<u64>,
			outputs_count: Option<u64>,
			outputs_depth: Option<u64>,
			outputs_weight: Option<u64>,
		}
		let statement = formatdoc!(
			"
				select
					children_count,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_count,
					outputs_depth,
					outputs_weight
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0)
			.map(|row| {
				let children = tg::process::metadata::Children {
					count: row.children_count,
				};
				let commands = tg::object::Metadata {
					count: row.commands_count,
					depth: row.commands_depth,
					weight: row.commands_weight,
				};
				let outputs = tg::object::Metadata {
					count: row.outputs_count,
					depth: row.outputs_depth,
					weight: row.outputs_weight,
				};
				tg::process::Metadata {
					children,
					commands,
					outputs,
				}
			});

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) fn try_get_process_metadata_sqlite_sync(
		index: &sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let statement = indoc!(
			"
				select
					children_count,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_count,
					outputs_depth,
					outputs_weight
				from processes
				where id = ?1;
			"
		);
		let mut statement = index
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([id.to_bytes().to_vec()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let children_count = row
			.get::<_, Option<u64>>(0)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let commands_count = row
			.get::<_, Option<u64>>(1)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let commands_depth = row
			.get::<_, Option<u64>>(2)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let commands_weight = row
			.get::<_, Option<u64>>(3)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let outputs_count = row
			.get::<_, Option<u64>>(4)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let outputs_depth = row
			.get::<_, Option<u64>>(5)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let outputs_weight = row
			.get::<_, Option<u64>>(6)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children = tg::process::metadata::Children {
			count: children_count,
		};
		let commands = tg::object::Metadata {
			count: commands_count,
			depth: commands_depth,
			weight: commands_weight,
		};
		let outputs = tg::object::Metadata {
			count: outputs_count,
			depth: outputs_depth,
			weight: outputs_weight,
		};
		let metadata = tg::process::Metadata {
			children,
			commands,
			outputs,
		};
		Ok(Some(metadata))
	}

	async fn try_get_process_metadata_remote(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| async move { client.get_process_metadata(id).await }.boxed())
			.collect::<Vec<_>>();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((metadata, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	pub(crate) async fn handle_get_process_metadata_request<H>(
		handle: &H,
		_request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_process_metadata(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
