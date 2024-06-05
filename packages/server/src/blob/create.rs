use crate::{database::Transaction, tmp::Tmp, Server};
use bytes::Bytes;
use either::Either;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::{
	pin::pin,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};
use tangram_client as tg;
use tangram_database::{self as db, Connection as _, Database as _, Query as _, Transaction as _};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncRead, AsyncWriteExt};

use super::{AVG_LEAF_SIZE, MAX_BRANCH_CHILDREN, MAX_LEAF_SIZE, MIN_LEAF_SIZE};

pub struct Output {
	pub blob: tg::blob::Id,
	pub data: tg::blob::Data,
	pub weight: u64,
}

impl Server {
	pub async fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::blob::create::Output> {
		// Create the temp file.
		let tmp = Tmp::new(self);
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let file = tokio::fs::File::options()
			.create_new(true)
			.write(true)
			.open(&tmp.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open file for writing"))?;

		// Create the blob.
		let output = self
			.create_or_store_blob(reader, Either::Left(file))
			.await?;

		// Rename the file.
		tokio::fs::rename(&tmp.path, self.blobs_path().join(output.blob.to_string()))
			.await
			.map_err(|source| tg::error!(!source, "failed to rename file"))?;

		// Create the output.
		Ok(tg::blob::create::Output { blob: output.blob })
	}

	pub async fn try_store_blob(
		&self,
		blob: tg::blob::Id,
	) -> tg::error::Result<Option<tg::blob::Data>> {
		// Open the blob file.
		let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let path = self.blobs_path().join(blob.to_string());
		let file = match tokio::fs::File::open(&path).await {
			Ok(file) => file,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(error) => return Err(tg::error!(!error, "failed to open file")),
		};

		// Get a database connectino.
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database transaction"))?;

		// Store the blob.
		let output = self
			.create_or_store_blob(file, Either::Right(&transaction))
			.await?;

		// Check the id.
		if output.blob != blob {
			transaction
				.rollback()
				.await
				.map_err(|source| tg::error!(%source, "failed to rollback transaction"))?;
			return Err(tg::error!("invalid blob id"));
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Remove the file.
		tokio::fs::remove_file(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to remove the file"))?;

		// Return the blob data.
		Ok(Some(output.data))
	}

	pub async fn create_or_store_blob(
		&self,
		src: impl AsyncRead + Send + 'static,
		dst: Either<tokio::fs::File, &Transaction<'_>>,
	) -> tg::Result<Output> {
		// Wrap the destination.
		let dst = dst.map_left(|file| Arc::new(tokio::sync::Mutex::new(file)));

		// Create the reader.
		let reader = pin!(src);
		let mut reader = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);

		// Keep track of the most recent blob data that was created, and the total weight.
		let blob_data = Arc::new(std::sync::Mutex::new(None));
		let weight = Arc::new(AtomicU64::new(0));

		// Create the leaves.
		let mut children = reader
			.as_stream()
			.map_err(|source| tg::error!(!source, "failed to read from the reader"))
			.and_then(|chunk| async {
				// Create the leaf data.
				let bytes = Bytes::from(chunk.data);
				let size = bytes.len().to_u64().unwrap();
				let data = tg::leaf::Data { bytes };
				let id = tg::leaf::Id::new(&data.bytes);

				// Update the most recent blob data.
				blob_data.lock().unwrap().replace(data.clone().into());

				// Write to the destination.
				match &dst {
					Either::Left(file) => {
						let mut file = file.lock().await;
						file.write_all(&data.bytes).await.map_err(|source| {
							tg::error!(!source, "failed to write to blob file")
						})?;
					},
					Either::Right(transaction) => {
						let p = transaction.p();
						let statement = formatdoc!(
							"
									insert into objects (id, bytes, touched_at, count, weight)
									values ({p}1, {p}2, {p}3, {p}4, {p}5)
									on conflict (id) do update set touched_at = {p}3;
								"
						);
						let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
						let params = db::params![&id, &data.bytes, now, size, size];
						transaction
							.execute(statement, params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
					},
				}

				// Create the child data.
				let blob = id.into();
				let child = tg::branch::child::Data { blob, size };

				// Update the weight.
				weight.fetch_add(size, Ordering::Relaxed);

				Ok::<_, tg::Error>(child)
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Create the tree.
		while children.len() > MAX_BRANCH_CHILDREN {
			children = stream::iter(children)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(async {
							// Create the branch data.
							let size = chunk.iter().map(|blob| blob.size).sum();
							let data = tg::branch::Data { children: chunk };
							let bytes = data.serialize()?;
							let id = tg::branch::Id::new(&bytes);

							// Update the most recent data.
							blob_data.lock().unwrap().replace(data.clone().into());

							// Write to the destination if necessary.
							if let Either::Right(transaction) = &dst {
								let p = transaction.p();
								let statement = formatdoc!(
									"
										insert into objects (id, bytes, touched_at, count, weight)
										values ({p}1, {p}2, {p}3, {p}4, {p}5)
										on conflict (id) do update set touched_at = {p}3;
									"
								);
								let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
								let weight = size + bytes.len().to_u64().unwrap();
								let params = db::params![&id, &bytes, now, weight, weight];
								transaction
									.execute(statement, params)
									.await
									.map_err(|source| {
										tg::error!(!source, "failed to execute the statement")
									})?;
							}
							// Create the child data.
							let blob = id.into();
							let child = tg::branch::child::Data { blob, size };

							// Update the weight.
							weight.fetch_add(bytes.len().to_u64().unwrap(), Ordering::Relaxed);

							Ok::<_, tg::Error>(child)
						})
						.left_stream()
					} else {
						stream::iter(chunk.into_iter().map(Ok)).right_stream()
					}
				})
				.try_collect()
				.await?;
		}

		// Get the id and data.
		let blob = match children.len() {
			0 => tg::leaf::Id::new(&Bytes::default()).into(),
			1 => children[0].blob.clone(),
			_ => {
				let data = tg::branch::Data { children };
				tg::branch::Id::new(&data.serialize()?).into()
			},
		};

		let data = blob_data
			.lock()
			.unwrap()
			.clone()
			.unwrap_or(tg::blob::Data::Leaf(tg::leaf::Data {
				bytes: Bytes::default(),
			}));
		let weight = weight.load(Ordering::Relaxed);
		Ok(Output { blob, data, weight })
	}
}

impl Server {
	pub(crate) async fn handle_create_blob_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let output = handle.create_blob(request.reader()).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
