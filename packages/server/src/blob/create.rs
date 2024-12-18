use crate::{database::Transaction, Server};
use bytes::Bytes;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::{pin::pin, sync::Arc};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncRead, AsyncWriteExt as _};

const MAX_BRANCH_CHILDREN: usize = 1_024;
const MIN_LEAF_SIZE: u32 = 4_096;
const AVG_LEAF_SIZE: u32 = 65_536;
const MAX_LEAF_SIZE: u32 = 131_072;

#[derive(Clone, Debug)]
pub struct InnerOutput {
	pub blob: tg::blob::Id,
	pub count: u64,
	pub depth: u64,
	pub size: u64,
	pub weight: u64,
}

impl Server {
	pub(crate) async fn create_blob(
		&self,
		reader: impl AsyncRead,
	) -> tg::Result<tg::blob::create::Output> {
		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Create a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database transaction"))?;

		// Create the blob.
		let InnerOutput {
			blob,
			count,
			depth,
			weight,
			..
		} = self
			.create_blob_inner(reader, Some(Either::Right(&transaction)))
			.await?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Create the metadata.
		let metadata = tg::object::Metadata {
			complete: true,
			count: Some(count),
			weight: Some(weight),
			depth: Some(depth),
		};

		// Create the output.
		Ok(tg::blob::create::Output { blob, metadata })
	}

	pub(crate) async fn create_blob_inner(
		&self,
		src: impl AsyncRead,
		dst: Option<Either<tokio::fs::File, &Transaction<'_>>>,
	) -> tg::Result<InnerOutput> {
		// Wrap the destination.
		let dst = dst.map(|dst| dst.map_left(|file| Arc::new(tokio::sync::Mutex::new(file))));

		// Create the reader.
		let reader = pin!(src);
		let mut reader = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);

		// Create the leaves.
		let mut output = reader
			.as_stream()
			.map_err(|source| tg::error!(!source, "failed to read from the reader"))
			.and_then(|chunk| async {
				// Create the leaf data.
				let bytes = Bytes::from(chunk.data);
				let size = bytes.len().to_u64().unwrap();
				let data = tg::leaf::Data { bytes };
				let id = tg::leaf::Id::new(&data.bytes);

				// Write to the destination.
				match &dst {
					Some(Either::Left(file)) => {
						let mut file = file.lock().await;
						file.write_all(&data.bytes).await.map_err(|source| {
							tg::error!(!source, "failed to write to blob file")
						})?;
					},
					Some(Either::Right(transaction)) => {
						let p = transaction.p();
						let statement = formatdoc!(
							"
								insert into objects (id, bytes, complete, count, depth, weight, touched_at)
								values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
								on conflict (id) do update set touched_at = {p}7;
							"
						);
						let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
						let params = db::params![&id, &data.bytes, 1, 1, 1, size, now];
						transaction
							.execute(statement, params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
					},
					None => (),
				}

				// Create the child data.
				let blob = id.into();

				// Update the count and weight.
				let output = InnerOutput {
					blob,
					count: 1,
					depth: 1,
					size,
					weight: size,
				};

				Ok::<_, tg::Error>(output)
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Create the tree.
		while output.len() > MAX_BRANCH_CHILDREN {
			output = stream::iter(output)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(async {
							// Create the branch data.
							let (size, count, depth, weight) = chunk.iter().fold(
								(0, 0, 0, 0),
								|(size, count, depth, weight), output| {
									(
										size + output.size,
										count + output.count,
										depth.max(output.depth),
										weight + output.weight,
									)
								},
							);
							let children = chunk
								.into_iter()
								.map(|output| tg::branch::data::Child {
									blob: output.blob,
									size: output.size,
								})
								.collect();
							let data = tg::branch::Data { children };
							let bytes = data.serialize()?;
							let id = tg::branch::Id::new(&bytes);
							let count = count + 1;
							let depth = depth + 1;
							let weight = weight + bytes.len().to_u64().unwrap();

							// Write to the destination if necessary.
							if let Some(Either::Right(transaction)) = &dst {
								let p = transaction.p();
								let statement = formatdoc!(
									"
										insert into objects (id, bytes, complete, count, depth, weight, touched_at)
										values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
										on conflict (id) do update set touched_at = {p}7;
									"
								);
								let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
								let params = db::params![&id, &bytes, 1, count, depth, weight, now];
								transaction
									.execute(statement, params)
									.await
									.map_err(|source| {
										tg::error!(!source, "failed to execute the statement")
									})?;
							}

							// Create the child data.
							let blob = id.into();
							let output = InnerOutput {
								blob,
								count,
								depth,
								size,
								weight,
							};

							Ok::<_, tg::Error>(output)
						})
						.left_stream()
					} else {
						stream::iter(chunk.into_iter().map(Ok)).right_stream()
					}
				})
				.try_collect()
				.await?;
		}

		// Create the output.
		let output = match output.len() {
			0 => {
				// Create the blob data.
				let bytes = Bytes::default();
				let blob = tg::leaf::Id::new(&bytes).into();
				let count = 1;
				let depth = 1;
				let weight = 0;

				// Write to the destination if necessary.
				if let Some(Either::Right(transaction)) = &dst {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							insert into objects (id, bytes, complete, count, depth, weight, touched_at)
							values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
							on conflict (id) do update set touched_at = {p}7;
						"
					);
					let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
					let params = db::params![&blob, &bytes, 1, count, depth, weight, now];
					transaction
						.execute(statement, params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}

				InnerOutput {
					blob,
					count: 1,
					depth: 1,
					size: 0,
					weight: 0,
				}
			},

			1 => output[0].clone(),

			_ => {
				// Get the size, count, depth, weight, and children.
				let (size, count, depth, weight) =
					output
						.iter()
						.fold((0, 0, 0, 0), |(size, count, depth, weight), output| {
							(
								size + output.size,
								count + output.count,
								std::cmp::max(depth, output.depth),
								weight + output.weight,
							)
						});
				let children = output
					.into_iter()
					.map(|output| tg::branch::data::Child {
						blob: output.blob,
						size: output.size,
					})
					.collect();

				// Create the blob data.
				let data = tg::branch::Data { children };
				let bytes = data.serialize()?;
				let blob = tg::branch::Id::new(&bytes).into();
				let count = count + 1;
				let depth = depth + 1;
				let weight = weight + bytes.len().to_u64().unwrap();

				// Write to the destination if necessary.
				if let Some(Either::Right(transaction)) = &dst {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							insert into objects (id, bytes, complete, count, depth, weight, touched_at)
							values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
							on conflict (id) do update set touched_at = {p}7;
						"
					);
					let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
					let params = db::params![&blob, &bytes, 1, count, depth, weight, now];
					transaction
						.execute(statement, params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}

				InnerOutput {
					blob,
					count,
					depth,
					size,
					weight,
				}
			},
		};

		Ok(output)
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
