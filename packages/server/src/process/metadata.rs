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
		#[derive(db::row::Deserialize)]
		struct Row {
			node_command_count: Option<u64>,
			node_command_depth: Option<u64>,
			node_command_size: Option<u64>,
			node_output_count: Option<u64>,
			node_output_depth: Option<u64>,
			node_output_size: Option<u64>,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_count: Option<u64>,
		}
		let statement = formatdoc!(
			"
				select
					node_command_count,
					node_command_depth,
					node_command_size,
					node_output_count,
					node_output_depth,
					node_output_size,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_count
				from processes
				where processes.id = {p}1;
			"
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| {
				let node = tg::process::metadata::Node {
					command: tg::object::metadata::Subtree {
						count: row.node_command_count,
						depth: row.node_command_depth,
						size: row.node_command_size,
						solvable: None,
						solved: None,
					},
					output: tg::object::metadata::Subtree {
						count: row.node_output_count,
						depth: row.node_output_depth,
						size: row.node_output_size,
						solvable: None,
						solved: None,
					},
				};
				let subtree = tg::process::metadata::Subtree {
					command: tg::object::metadata::Subtree {
						count: row.subtree_command_count,
						depth: row.subtree_command_depth,
						size: row.subtree_command_size,
						solvable: None,
						solved: None,
					},
					output: tg::object::metadata::Subtree {
						count: row.subtree_output_count,
						depth: row.subtree_output_depth,
						size: row.subtree_output_size,
						solvable: None,
						solved: None,
					},
					count: row.subtree_count,
				};
				tg::process::Metadata { node, subtree }
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
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			node_command_count: Option<u64>,
			node_command_depth: Option<u64>,
			node_command_size: Option<u64>,
			node_output_count: Option<u64>,
			node_output_depth: Option<u64>,
			node_output_size: Option<u64>,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_count: Option<u64>,
		}
		let statement = indoc!(
			"
				select
					node_command_count,
					node_command_depth,
					node_command_size,
					node_output_count,
					node_output_depth,
					node_output_size,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_count
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
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
		let node = tg::process::metadata::Node {
			command: tg::object::metadata::Subtree {
				count: row.node_command_count,
				depth: row.node_command_depth,
				size: row.node_command_size,
				solvable: None,
				solved: None,
			},
			output: tg::object::metadata::Subtree {
				count: row.node_output_count,
				depth: row.node_output_depth,
				size: row.node_output_size,
				solvable: None,
				solved: None,
			},
		};
		let subtree = tg::process::metadata::Subtree {
			command: tg::object::metadata::Subtree {
				count: row.subtree_command_count,
				depth: row.subtree_command_depth,
				size: row.subtree_command_size,
				solvable: None,
				solved: None,
			},
			output: tg::object::metadata::Subtree {
				count: row.subtree_output_count,
				depth: row.subtree_output_depth,
				size: row.subtree_output_size,
				solvable: None,
				solved: None,
			},
			count: row.subtree_count,
		};
		let metadata = tg::process::Metadata { node, subtree };
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
