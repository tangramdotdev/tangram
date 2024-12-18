use crate::Server;
use bytes::Bytes;
use futures::{future, FutureExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

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

	pub async fn try_get_object_local(
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
			bytes: Option<Bytes>,
			complete: bool,
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select bytes, complete, count, depth, weight
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let Some(row) = connection
			.query_optional_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let Some(bytes) = row.bytes else {
			return Ok(None);
		};

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::object::get::Output {
			bytes,
			metadata: tg::object::Metadata {
				complete: row.complete,
				count: row.count,
				depth: row.depth,
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
		let response = http::Response::builder()
			.header_json(tg::object::metadata::HEADER, output.metadata)
			.unwrap()
			.bytes(output.bytes)
			.unwrap();
		Ok(response)
	}
}
