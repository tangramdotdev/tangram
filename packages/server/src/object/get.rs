use crate::Server;
use bytes::Bytes;
use futures::{FutureExt as _, future};
use indoc::formatdoc;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};
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
		let mut bytes = self.store.try_get(id).await?;

		// If the bytes were not in the store, then attempt to read the bytes from a blob file.
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
		let output = tg::object::get::Output { bytes };

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
			blob: tg::blob::Id,
			position: u64,
			length: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select blob, position, length 
				from blob_references 
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
		let mut file =
			match tokio::fs::File::open(self.blobs_path().join(row.blob.to_string())).await {
				Ok(file) => file,
				Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
				Err(error) => {
					return Err(tg::error!(
						!error,
						"failed to open the entry in the blobs directory"
					));
				},
			};
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
		_request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_object(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().bytes(output.bytes).unwrap();
		Ok(response)
	}
}
