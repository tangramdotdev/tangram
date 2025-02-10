use crate::{database::Transaction, temp::Temp, Server};
use bytes::Bytes;
use dashmap::DashMap;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive;
use std::{os::unix::fs::PermissionsExt as _, path::Path, pin::pin};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{request::Ext as _, response::builder::Ext as _, Body};
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

struct State<'a> {
	entries: DashMap<tg::blob::Id, (u64, u64), fnv::FnvBuildHasher>,
	transaction: Transaction<'a>,
	write_to_blobs_table: bool,
}

impl Server {
	pub(crate) async fn create_blob_with_path(
		&self,
		path: &Path,
	) -> tg::Result<tg::blob::create::Output> {
		// If the server is configured to store blobs in the database, then open the file and create a blob with the file.
		if !self.config.advanced.write_blobs_to_blobs_directory {
			let reader = tokio::fs::File::open(path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to open blob for reading"),
			)?;
			return self.create_blob_with_reader(reader).await;
		}

		// Open the file.
		let file = tokio::fs::File::open(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
		)?;

		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Create the state.
		let state = State {
			entries: DashMap::default(),
			transaction,
			write_to_blobs_table: true,
		};

		// Create the blob.
		let InnerOutput {
			blob,
			count,
			depth,
			weight,
			..
		} = self.create_blob_inner(&state, file).await?;

		// Commit the transaction.
		state
			.transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Copy the file to the blobs directory through a temp if it is not already present.
		let blob_path = self.blobs_path().join(blob.to_string());
		if !tokio::fs::try_exists(&blob_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the blob path exists"))?
		{
			let temp = Temp::new(self);
			tokio::fs::copy(path, temp.path())
				.await
				.map_err(|source| tg::error!(!source, "failed to copy the file"))?;
			let permissions = std::fs::Permissions::from_mode(0o644);
			tokio::fs::set_permissions(temp.path(), permissions)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
			tokio::fs::rename(temp.path(), blob_path)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to rename the file to the blobs directory")
				})?;
		}

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

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database transaction"))?;

		// Create the state.
		let state = State {
			entries: DashMap::default(),
			transaction,
			write_to_blobs_table: false,
		};

		// Create the blob.
		let InnerOutput {
			blob,
			count,
			depth,
			weight,
			..
		} = self.create_blob_inner(&state, reader).await?;

		// Commit the transaction.
		state
			.transaction
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

	async fn create_blob_inner(
		&self,
		state: &State<'_>,
		reader: impl AsyncRead,
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
		let mut outputs = reader
			.as_stream()
			.map_err(|source| tg::error!(!source, "failed to read from the reader"))
			.and_then(|chunk| self.create_blob_inner_leaf(state, chunk))
			.try_collect::<Vec<_>>()
			.await?;

		// Create the tree.
		while outputs.len() > MAX_BRANCH_CHILDREN {
			outputs = stream::iter(outputs)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(self.create_blob_inner_branch(state, chunk)).left_stream()
					} else {
						stream::iter(chunk.into_iter().map(Ok)).right_stream()
					}
				})
				.try_collect()
				.await?;
		}

		// Get the output.
		let output = match outputs.len() {
			0 => self.create_blob_inner_empty_leaf(state).await?,
			1 => outputs[0].clone(),
			_ => self.create_blob_inner_branch(state, outputs).await?,
		};

		// Write to the blobs table if necessary.
		if state.write_to_blobs_table {
			for entry in &state.entries {
				let blob = entry.key();
				let (position, length) = entry.value();
				let p = state.transaction.p();
				let statement = formatdoc!(
					"
						insert into blobs (id, entry, position, length)
						values ({p}1, {p}2, {p}3, {p}4)
						on conflict (id) do update set entry = {p}2, position = {p}3, length = {p}4;
					"
				);
				let params = db::params![blob, &output.blob, position, length];
				state
					.transaction
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		Ok(output)
	}

	async fn create_blob_inner_empty_leaf(&self, state: &State<'_>) -> tg::Result<InnerOutput> {
		let bytes = Bytes::default();
		let id = tg::leaf::Id::new(&bytes).into();
		let p = state.transaction.p();
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, complete, count, depth, incomplete_children, size, touched_at, weight)
				values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7, {p}8, {p}9)
				on conflict (id) do update set touched_at = {p}8;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![&id, &bytes, 1, 1, 1, 0, 0, now, 0];
		state
			.transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(InnerOutput {
			blob: id,
			count: 1,
			depth: 1,
			size: 0,
			weight: 0,
		})
	}

	async fn create_blob_inner_leaf(
		&self,
		state: &State<'_>,
		chunk: fastcdc::v2020::ChunkData,
	) -> tg::Result<InnerOutput> {
		let fastcdc::v2020::ChunkData {
			offset: position,
			length: size,
			data,
			..
		} = chunk;
		let bytes = data.into();
		let size = size.to_u64().unwrap();
		let id = tg::leaf::Id::new(&bytes);
		let p = state.transaction.p();
		let bytes = (!state.write_to_blobs_table).then_some(&bytes);
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, complete, count, depth, incomplete_children, size, touched_at, weight)
				values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7, {p}8, {p}9)
				on conflict (id) do update set touched_at = {p}8;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![&id, &bytes, 1, 1, 1, 0, size, now, size];
		state
			.transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		state.entries.insert(id.clone().into(), (position, size));
		let output = InnerOutput {
			blob: id.into(),
			count: 1,
			depth: 1,
			size,
			weight: size,
		};
		Ok(output)
	}

	async fn create_blob_inner_branch(
		&self,
		state: &State<'_>,
		children: Vec<InnerOutput>,
	) -> tg::Result<InnerOutput> {
		let (size, count, depth, weight) =
			children
				.iter()
				.fold((0, 0, 0, 0), |(size, count, depth, weight), output| {
					(
						size + output.size,
						count + output.count,
						depth.max(output.depth),
						weight + output.weight,
					)
				});
		let children = children
			.into_iter()
			.map(|output| tg::branch::data::Child {
				blob: output.blob.clone(),
				size: output.size,
			})
			.collect();
		let data = tg::branch::Data { children };
		let bytes = data.serialize()?;
		let size_ = bytes.len().to_u64().unwrap();
		let count = 1 + count;
		let depth = 1 + depth;
		let weight = size_ + weight;
		let id = tg::branch::Id::new(&bytes);
		let p = state.transaction.p();
		for child in data.children() {
			let statement = formatdoc!(
				"
					insert into object_children (object, child)
					values ({p}1, {p}2)
					on conflict (object, child) do nothing;
				"
			);
			let params = db::params![&id, child.to_string()];
			state
				.transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}
		let statement = formatdoc!(
			"
				insert into objects (id, bytes, complete, count, depth, incomplete_children, size, touched_at, weight)
				values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7, {p}8, {p}9)
				on conflict (id) do update set touched_at = {p}8;
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![&id, &bytes, 1, count, depth, 0, size_, now, weight];
		state
			.transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let output = InnerOutput {
			blob: id.into(),
			count,
			depth,
			size,
			weight,
		};
		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_create_blob_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let output = handle.create_blob(request.reader()).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
