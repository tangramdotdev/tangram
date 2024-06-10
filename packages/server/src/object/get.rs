use crate::Server;
use bytes::Bytes;
use futures::{future, FutureExt as _, TryFutureExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_futures::task::Stop;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};
use tg::Handle as _;

impl Server {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		if let Some(output) = self.try_get_object_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_object_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	async fn try_get_object_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Try to read from the database.
		if let Some(output) = self.try_get_object_local_database(id).await? {
			return Ok(Some(output));
		};

		// Avoid creating an object store task for objets that aren't blobs or artifacts.
		if !matches!(
			id,
			tg::object::Id::Leaf(_)
				| tg::object::Id::Branch(_)
				| tg::object::Id::Directory(_)
				| tg::object::Id::File(_)
				| tg::object::Id::Symlink(_)
		) {
			return Ok(None);
		}

		// Get the object creation task.
		let create_task = {
			let id = id.clone();
			let server = self.clone();
			|_stop: Stop| async move {
				match id {
					tg::object::Id::Leaf(leaf) => {
						let blob = tg::blob::Id::Leaf(leaf);
						server.try_store_blob(blob).await
					},
					tg::object::Id::Branch(branch) => {
						let blob = tg::blob::Id::Branch(branch);
						server.try_store_blob(blob).await
					},
					tg::object::Id::Directory(directory) => {
						let artifact = tg::artifact::Id::Directory(directory);
						server.try_store_artifact(&artifact).await
					},
					tg::object::Id::File(file) => {
						let artifact = tg::artifact::Id::File(file);
						server.try_store_artifact(&artifact).await
					},
					tg::object::Id::Symlink(symlink) => {
						let artifact = tg::artifact::Id::Symlink(symlink);
						server.try_store_artifact(&artifact).await
					},
					_ => unreachable!(),
				}
			}
		};

		// Check if the object is stored yet.
		let stored = self
			.object_store_tasks
			.get_or_spawn(id.clone(), create_task)
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for task"))??;
		if stored {
			let output = self.try_get_object_local_database(id).await?.unwrap();
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub async fn try_get_object_local_database(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		// Get the object.
		#[derive(serde::Deserialize)]
		struct Row {
			bytes: Bytes,
			count: Option<u64>,
			weight: Option<u64>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select bytes, count, weight
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let Some(row) = connection
			.query_optional_into::<Row>(statement, params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))
			.await?
		else {
			return Ok(None);
		};

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::object::get::Output {
			bytes: row.bytes,
			metadata: tg::object::Metadata {
				count: row.count,
				weight: row.weight,
			},
		};
		Ok(Some(output))
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Attempt to get the object from the remotes.
		let futures = self
			.remotes
			.iter()
			.map(|remote| async move { remote.get_object(id).await }.boxed())
			.collect_vec();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((output, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		// Spawn a task to put the object.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let output = output.clone();
			async move {
				let arg = tg::object::put::Arg {
					bytes: output.bytes.clone(),
				};
				server.put_object(&id, arg).await?;
				Ok::<_, tg::Error>(())
			}
		});

		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_get_object_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_object(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let mut response = http::Response::builder();
		response = response
			.header_json(tg::object::metadata::HEADER, output.metadata)
			.unwrap();
		let response = response.bytes(output.bytes).unwrap();
		Ok(response)
	}
}
