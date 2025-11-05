#[cfg(feature = "postgres")]
use indoc::formatdoc;
use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn try_get_process_metadata_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		mut arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let output = remote.try_get_process_metadata(id, arg).await?;
			return Ok(output);
		}

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
	pub(crate) async fn try_get_process_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		// Get a connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get an index connection"))?;

		// Get the process metadata.
		let p = connection.p();
		#[derive(serde::Deserialize)]
		struct Row {
			children_count: Option<u64>,
			command_count: Option<u64>,
			command_depth: Option<u64>,
			command_weight: Option<u64>,
			children_commands_count: Option<u64>,
			children_commands_depth: Option<u64>,
			children_commands_weight: Option<u64>,
			children_outputs_count: Option<u64>,
			children_outputs_depth: Option<u64>,
			children_outputs_weight: Option<u64>,
			output_count: Option<u64>,
			output_depth: Option<u64>,
			output_weight: Option<u64>,
		}
		let statement = formatdoc!(
			"
				select
					children_count,
					command_count,
					command_depth,
					command_weight,
					children_commands_count,
					children_commands_depth,
					children_commands_weight,
					children_outputs_count,
					children_outputs_depth,
					children_outputs_weight,
					output_count,
					output_depth,
					output_weight
				from processes
				where processes.id = {p}1;
			"
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
				let command = tg::object::Metadata {
					count: row.command_count,
					depth: row.command_depth,
					weight: row.command_weight,
				};
				let children_commands = tg::object::Metadata {
					count: row.children_commands_count,
					depth: row.children_commands_depth,
					weight: row.children_commands_weight,
				};
				let output = tg::object::Metadata {
					count: row.output_count,
					depth: row.output_depth,
					weight: row.output_weight,
				};
				let children_outputs = tg::object::Metadata {
					count: row.children_outputs_count,
					depth: row.children_outputs_depth,
					weight: row.children_outputs_weight,
				};
				tg::process::Metadata {
					children,
					children_commands,
					children_outputs,
					command,
					output,
				}
			});

		// Drop the index connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_process_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| Self::try_get_process_metadata_sqlite_sync(connection, &id)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let statement = indoc!(
			"
				select
					children_count,
					command_count,
					command_depth,
					command_weight,
					children_commands_count,
					children_commands_depth,
					children_commands_weight,
					children_outputs_count,
					children_outputs_depth,
					children_outputs_weight,
					output_count,
					output_depth,
					output_weight
				from processes
				where processes.id = ?1;
			"
		);
		let mut statement = connection
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
		let command_count = row
			.get::<_, Option<u64>>(1)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let command_depth = row
			.get::<_, Option<u64>>(2)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let command_weight = row
			.get::<_, Option<u64>>(3)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_commands_count = row
			.get::<_, Option<u64>>(4)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_commands_depth = row
			.get::<_, Option<u64>>(5)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_commands_weight = row
			.get::<_, Option<u64>>(6)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_outputs_count = row
			.get::<_, Option<u64>>(7)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_outputs_depth = row
			.get::<_, Option<u64>>(8)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_outputs_weight = row
			.get::<_, Option<u64>>(9)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let output_count = row
			.get::<_, Option<u64>>(10)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let output_depth = row
			.get::<_, Option<u64>>(11)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let output_weight = row
			.get::<_, Option<u64>>(12)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children = tg::process::metadata::Children {
			count: children_count,
		};
		let command = tg::object::Metadata {
			count: command_count,
			depth: command_depth,
			weight: command_weight,
		};
		let children_commands = tg::object::Metadata {
			count: children_commands_count,
			depth: children_commands_depth,
			weight: children_commands_weight,
		};
		let output = tg::object::Metadata {
			count: output_count,
			depth: output_depth,
			weight: output_weight,
		};
		let children_outputs = tg::object::Metadata {
			count: children_outputs_count,
			depth: children_outputs_depth,
			weight: children_outputs_weight,
		};
		let metadata = tg::process::Metadata {
			children,
			children_commands,
			children_outputs,
			command,
			output,
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

	pub(crate) async fn handle_get_process_metadata_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let Some(output) = self
			.try_get_process_metadata_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
