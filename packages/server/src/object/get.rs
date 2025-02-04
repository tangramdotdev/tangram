use crate::Server;
use bytes::Bytes;
use futures::{future, FutureExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use tangram_client::{self as tg, handle::Ext as _, object::Metadata};
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

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

		// Create the store future.
		let store = async {
			// Attempt to get the bytes from the store.
			if let Some(store) = &self.store {
				let bytes = store.try_get(id).await?;
				return Ok::<(Option<Bytes>, Option<Metadata>), tg::Error>((bytes, None));
			}
			Ok::<_, tg::Error>((None, None))
		};

		// Create the database future.
		let database = async {
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
			let row = connection
				.query_optional_into::<Row>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the database connection.
			drop(connection);

			// Create the bytes and metadata.
			let mut bytes = None;
			let mut metadata = tg::object::Metadata::default();

			// If the row was in the database, then get the values.
			if let Some(row) = row {
				bytes = row.bytes;
				metadata.complete = row.complete;
				metadata.count = row.count;
				metadata.depth = row.depth;
				metadata.weight = row.weight;
			};
			Ok::<_, tg::Error>((bytes, metadata))
		};

		// Await the futures.
		let ((store_bytes, _), (database_bytes, metadata)) = futures::try_join!(store, database)?;
		let mut bytes = store_bytes.or(database_bytes);

		// If the bytes were not in the database or the store, then attempt to read the bytes from a blob file.
		if bytes.is_none() {
			if let Ok(id) = id.try_unwrap_leaf_ref() {
				bytes = self.try_read_leaf_from_blobs_directory(id).await?;
			}
		}

		// If the bytes were not found, then return None.
		let Some(bytes) = bytes else {
			return Ok(None);
		};

		// Create the output.
		let output = tg::object::get::Output { bytes, metadata };

		Ok(Some(output))
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Attempt to get the object from the remotes.
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| async move { client.get_object(id).await }.boxed())
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

	async fn try_read_leaf_from_blobs_directory(
		&self,
		id: &tg::leaf::Id,
	) -> tg::Result<Option<Bytes>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		#[derive(Debug, serde::Deserialize)]
		struct Row {
			entry: tg::blob::Id,
			position: u64,
			length: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select entry, position, length 
				from blobs 
				where id = {p}1
			"
		);
		let params = db::params![id];
		let Some(row) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Drop the connection.
		drop(connection);

		// Read the leaf from the file.
		let mut file = tokio::fs::File::open(self.blobs_path().join(row.entry.to_string()))
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to find the entry in the blobs directory")
			})?;
		file.seek(std::io::SeekFrom::Start(row.position))
			.await
			.map_err(|source| tg::error!(!source, "failed to seek in the blob file"))?;
		let mut buffer = vec![0; row.length.to_usize().unwrap()];
		file.read_exact(&mut buffer)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the leaf from the file"))?;

		Ok(Some(buffer.into()))
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
