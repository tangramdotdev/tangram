use crate::{database::Transaction, temp::Temp, Server};
use bytes::Bytes;
use dashmap::DashMap;
use fastcdc::v2020::ChunkData;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::{path::Path, pin::pin};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncRead;

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

// Helper struct to keep track of the state used while writing blobs to the database table(s).
struct Writer<'a> {
	offsets: DashMap<tg::blob::Id, (u64, u64), fnv::FnvBuildHasher>,
	transaction: Transaction<'a>,
	write_offsets_to_blobs_table: bool,
}

impl Server {
	pub(crate) async fn create_blob_with_path(
		&self,
		path: &Path,
	) -> tg::Result<tg::blob::create::Output> {
		// If configured to store blobs in the database, don't attempt to write to the cache directory.
		if !self.config.advanced.write_blobs_to_cache_directory {
			let reader = tokio::fs::File::open(path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to open blob for reading"),
			)?;
			return self.create_blob_with_reader(reader).await;
		}

		// Copy the blob to a temp.
		let temp = Temp::new(self);
		tokio::fs::copy(path, temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to copy the blob"))?;

		// Create the reader.
		let reader = tokio::fs::File::open(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to open temp"))?;

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

		// Create the writer.
		let writer = Writer {
			offsets: DashMap::default(),
			transaction,
			write_offsets_to_blobs_table: true,
		};

		// Create the blob.
		let InnerOutput {
			blob,
			count,
			depth,
			weight,
			..
		} = self.create_blob_inner(reader, writer).await?;

		// Move the temp to the output.
		tokio::fs::rename(temp.path(), self.cache_path().join(blob.to_string()))
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to move the blob to the cache directory")
			})?;

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

	pub(crate) async fn create_blob_with_reader(
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

		// Create the writer.
		let writer = Writer {
			offsets: DashMap::default(),
			transaction,
			write_offsets_to_blobs_table: false,
		};

		// Create the blob.
		let InnerOutput {
			blob,
			count,
			depth,
			weight,
			..
		} = self.create_blob_inner(reader, writer).await?;

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

	async fn create_blob_inner(
		&self,
		reader: impl AsyncRead,
		writer: Writer<'_>,
	) -> tg::Result<InnerOutput> {
		// Create the reader.
		let reader = pin!(reader);
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
			.and_then(|chunk| writer.write_leaf(chunk))
			.try_collect::<Vec<_>>()
			.await?;

		// Create the tree.
		while output.len() > MAX_BRANCH_CHILDREN {
			output = stream::iter(output)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(writer.write_branch(chunk)).left_stream()
					} else {
						stream::iter(chunk.into_iter().map(Ok)).right_stream()
					}
				})
				.try_collect()
				.await?;
		}

		// Get the output.
		writer.finish(output).await
	}
}

impl<'a> Writer<'a> {
	async fn write_leaf(&self, chunk: ChunkData) -> tg::Result<InnerOutput> {
		let ChunkData {
			offset,
			length,
			data,
			..
		} = chunk;
		let data = data.into();
		let length = length.to_u64().unwrap();

		// Compute the leaf ID.
		let id = tg::leaf::Id::new(&data);

		// Update the objects table.
		let p = self.transaction.p();
		let data = (!self.write_offsets_to_blobs_table).then_some(&data);
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, complete, count, depth, weight, touched_at)
				values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
				on conflict (id) do update set touched_at = {p}7;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![&id, &data, 1, 1, 1, length, now];
		self.transaction
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update the offsets table.
		self.offsets.insert(id.clone().into(), (offset, length));

		// Return the output.
		let output = InnerOutput {
			blob: id.into(),
			count: 1,
			depth: 1,
			weight: length,
			size: length,
		};
		Ok(output)
	}

	async fn write_branch(&self, chunk: Vec<InnerOutput>) -> tg::Result<InnerOutput> {
		// Create the branch data.
		let (size, count, depth, weight) =
			chunk
				.iter()
				.fold((0, 0, 0, 0), |(size, count, depth, weight), output| {
					(
						size + output.size,
						count + output.count,
						depth.max(output.depth),
						weight + output.weight,
					)
				});
		let children = chunk
			.into_iter()
			.map(|output| tg::branch::data::Child {
				blob: output.blob.clone(),
				size: output.size,
			})
			.collect();
		let data = tg::branch::Data { children };
		let bytes = data.serialize()?;
		let id = tg::branch::Id::new(&bytes);
		let count = count + 1;
		let depth = depth + 1;
		let weight = weight + bytes.len().to_u64().unwrap();

		let p = self.transaction.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, complete, count, depth, weight, touched_at)
				values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
				on conflict (id) do update set touched_at = {p}7;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![&id, &bytes, 1, count, depth, weight, now];
		self.transaction
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Create the child data.
		let blob = id.into();
		let output = InnerOutput {
			blob,
			count,
			depth,
			size,
			weight,
		};
		Ok(output)
	}

	async fn finish(self, chunk: Vec<InnerOutput>) -> tg::Result<InnerOutput> {
		// Get the output.
		let output = match chunk.len() {
			// Special case: the empty chunk.
			0 => {
				// Create the blob data.
				let bytes = Bytes::default();
				let blob = tg::leaf::Id::new(&bytes).into();
				let count = 1;
				let depth = 1;
				let weight = 0;

				let p = self.transaction.p();
				let statement = formatdoc!(
					"
						insert into objects (id, bytes, complete, count, depth, weight, touched_at)
						values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
						on conflict (id) do update set touched_at = {p}7;
					"
				);
				let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
				let params = db::params![&blob, &bytes, 1, count, depth, weight, now];
				self.transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				InnerOutput {
					blob,
					count: 1,
					depth: 1,
					size: 0,
					weight: 0,
				}
			},
			1 => chunk[0].clone(),
			_ => self.write_branch(chunk).await?,
		};

		// Write to the blobs table if necessary.
		if self.write_offsets_to_blobs_table {
			for entry in self.offsets.iter() {
				let blob = entry.key();
				let (position, length) = entry.value();
				let p = self.transaction.p();
				let statement = formatdoc!(
					"
						insert into blobs (id, root, offset, length)
						values ({p}1, {p}2, {p}3, {p}4)
					"
				);
				let params = db::params![blob, &output.blob, position, length];
				self.transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		// Commit the transaction.
		self.transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

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
