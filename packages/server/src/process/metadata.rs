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
		// Get a database connection.
		let connection = self
			.index
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the process metadata.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					commands_count,
					commands_depth,
					commands_weight,
					complete,
					count,
					outputs_count,
					outputs_depth,
					outputs_weight
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) fn try_get_process_metadata_local_sync(
		index: &sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let statement = indoc!(
			"
				select
					commands_count,
					commands_depth,
					commands_weight,
					count,
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
			.query([id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let commands_count = row
			.get::<_, Option<u64>>(0)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let commands_depth = row
			.get::<_, Option<u64>>(1)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let commands_weight = row
			.get::<_, Option<u64>>(2)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let count = row
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
		let metadata = tg::process::Metadata {
			commands_count,
			commands_depth,
			commands_weight,
			count,
			outputs_count,
			outputs_depth,
			outputs_weight,
		};
		Ok(Some(metadata))
	}

	async fn try_get_process_metadata_remote(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		// Attempt to get the process metadata from the remotes.
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
