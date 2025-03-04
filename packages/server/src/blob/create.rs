use crate::{Server, temp::Temp};
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use indoc::indoc;
use num::ToPrimitive;
use rusqlite as sqlite;
use std::{os::unix::fs::PermissionsExt as _, path::Path, pin::pin, sync::Arc};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio_postgres as postgres;

const MAX_BRANCH_CHILDREN: usize = 1_024;
const MIN_LEAF_SIZE: u32 = 4_096;
const AVG_LEAF_SIZE: u32 = 65_536;
const MAX_LEAF_SIZE: u32 = 131_072;

#[derive(Clone, Debug)]
pub struct Blob {
	pub children: Vec<Blob>,
	pub count: u64,
	pub data: Option<tg::blob::Data>,
	pub size: u64,
	pub depth: u64,
	pub id: tg::blob::Id,
	pub position: u64,
	pub length: u64,
	pub weight: u64,
}

pub enum Destination {
	Temp(Temp),
	Store,
}

impl Server {
	pub async fn create_blob(
		&self,
		reader: impl AsyncRead,
	) -> tg::Result<tg::blob::create::Output> {
		// Create the destination.
		let destination = if self.config.advanced.write_blobs_to_blobs_directory {
			Destination::Temp(Temp::new(self))
		} else {
			Destination::Store
		};

		// Create the blob.
		let blob = self.create_blob_inner(reader, Some(&destination)).await?;
		let blob = Arc::new(blob);

		// Rename the temp file to the blobs directory if necessary.
		if let Destination::Temp(temp) = destination {
			let blob_path = self.blobs_path().join(blob.id.to_string());
			tokio::fs::rename(temp.path(), blob_path)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to rename the file to the blobs directory")
				})?;
		}

		// Create the database future.
		let database_future = async { self.insert_blob(&blob).await };

		// Create the store future.
		let store_future = async { self.store_blob(&blob).await };

		// Join the database and store futures.
		futures::try_join!(database_future, store_future)?;

		// Create the output.
		let output = tg::blob::create::Output {
			blob: blob.id.clone(),
		};

		Ok(output)
	}

	pub(crate) async fn create_blob_with_path(
		&self,
		path: &Path,
	) -> tg::Result<tg::blob::create::Output> {
		// Open the file.
		let file = tokio::fs::File::open(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
		)?;

		// Create the blob.
		let blob = self.create_blob_inner(file, None).await?;
		let blob = Arc::new(blob);

		// Copy the file to the blobs directory through a temp if it is not already present.
		let blob_path = self.blobs_path().join(blob.id.to_string());
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

		// Create the database future.
		let database_future = async { self.insert_blob(&blob).await };

		// Create the store future.
		let store_future = async { self.store_blob(&blob).await };

		// Join the database and store futures.
		futures::try_join!(database_future, store_future)?;

		// Create the output.
		let output = tg::blob::create::Output {
			blob: blob.id.clone(),
		};

		Ok(output)
	}

	pub(crate) async fn create_blob_inner(
		&self,
		reader: impl AsyncRead,
		destination: Option<&Destination>,
	) -> tg::Result<Blob> {
		// Create the reader.
		let reader = pin!(reader);
		let mut reader = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);
		let stream = reader.as_stream();
		let mut stream = pin!(stream);

		// Open the destination file if necessary.
		let mut file = if let Some(Destination::Temp(temp)) = destination {
			Some(
				tokio::fs::File::create_new(temp.path())
					.await
					.map_err(|source| tg::error!(!source, "failed to create the file"))?,
			)
		} else {
			None
		};

		// Create the leaves and write or store them if necessary.
		let mut blobs = Vec::new();
		while let Some(chunk) = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to read from the reader"))?
		{
			// Create the leaf.
			let blob = self.create_blob_inner_leaf(&chunk).await?;

			// Store the leaf if necessary.
			match destination {
				None => (),
				Some(Destination::Temp(_)) => {
					file.as_mut()
						.unwrap()
						.write_all(&chunk.data)
						.await
						.map_err(|source| tg::error!(!source, "failed to write to the file"))?;
				},
				Some(Destination::Store) => {
					self.store
						.put(&blob.id.clone().into(), chunk.data.into())
						.await
						.map_err(|source| tg::error!(!source, "failed to store the leaf"))?;
				},
			}

			blobs.push(blob);
		}

		// Flush and close the file if necessary.
		if let Some(mut file) = file {
			file.flush()
				.await
				.map_err(|source| tg::error!(!source, "failed to flush the file"))?;
			drop(file);
		}

		// Create the tree.
		while blobs.len() > MAX_BRANCH_CHILDREN {
			blobs = stream::iter(blobs)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(self.create_blob_inner_branch(chunk)).left_stream()
					} else {
						stream::iter(chunk.into_iter().map(Ok)).right_stream()
					}
				})
				.try_collect()
				.await?;
		}

		// Get the blob.
		let blob = match blobs.len() {
			0 => self.create_blob_inner_empty_leaf().await?,
			1 => blobs[0].clone(),
			_ => self.create_blob_inner_branch(blobs).await?,
		};

		Ok(blob)
	}

	async fn create_blob_inner_empty_leaf(&self) -> tg::Result<Blob> {
		let bytes = Bytes::default();
		let id = tg::leaf::Id::new(&bytes);
		Ok(Blob {
			count: 1,
			depth: 1,
			weight: 0,
			children: Vec::new(),
			data: None,
			size: 0,
			id: id.into(),
			length: 0,
			position: 0,
		})
	}

	async fn create_blob_inner_leaf(&self, chunk: &fastcdc::v2020::ChunkData) -> tg::Result<Blob> {
		let fastcdc::v2020::ChunkData {
			offset: position,
			length,
			data,
			..
		} = chunk;
		let length = length.to_u64().unwrap();
		let size = length;
		let id = tg::leaf::Id::new(data);
		let count = 1;
		let depth = 1;
		let weight = length;
		let output = Blob {
			children: Vec::new(),
			count,
			data: None,
			size,
			depth,
			id: id.into(),
			position: *position,
			length,
			weight,
		};
		Ok(output)
	}

	async fn create_blob_inner_branch(&self, children: Vec<Blob>) -> tg::Result<Blob> {
		let children_ = children
			.iter()
			.map(|child| tg::branch::data::Child {
				blob: child.id.clone(),
				length: child.length,
			})
			.collect();
		let data = tg::branch::Data {
			children: children_,
		};
		let bytes = data.serialize()?;
		let size = bytes.len().to_u64().unwrap();
		let id = tg::branch::Id::new(&bytes);
		let (count, depth, weight) =
			children
				.iter()
				.fold((1, 1, size), |(count, depth, weight), child| {
					(
						count + child.count,
						depth.max(child.depth),
						weight + child.weight,
					)
				});
		let position = children.first().unwrap().position;
		let length = children.iter().map(|child| child.length).sum();
		let output = Blob {
			children,
			count,
			data: Some(data.into()),
			size,
			depth,
			id: id.into(),
			position,
			length,
			weight,
		};
		Ok(output)
	}

	async fn insert_blob(&self, blob: &Arc<Blob>) -> tg::Result<()> {
		let write_blobs_to_blobs_directory = self.config.advanced.write_blobs_to_blobs_directory;
		match &self.database {
			Either::Left(database) => {
				let connection = database
					.write_connection()
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

				connection
					.with({
						let blob = blob.clone();
						move |connection| {
							// Begin a transaction.
							let transaction = connection.transaction().map_err(|source| {
								tg::error!(!source, "failed to begin a transaction")
							})?;

							// Insert the blob.
							if write_blobs_to_blobs_directory {
								Self::insert_blob_sqlite(&blob, &transaction)?;
							}

							// Insert the objects.
							Self::insert_blob_objects_sqlite(&blob, &transaction)?;

							// Commit the transaction.
							transaction.commit().map_err(|source| {
								tg::error!(!source, "failed to commit the transaction")
							})?;

							Ok::<_, tg::Error>(())
						}
					})
					.await?;
			},
			Either::Right(database) => {
				let options = db::ConnectionOptions {
					kind: db::ConnectionKind::Write,
					priority: db::Priority::Low,
				};
				let mut connection = database
					.connection_with_options(options)
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

				// Begin a transaction.
				let transaction = connection
					.client_mut()
					.transaction()
					.await
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				// Insert the blob.
				if write_blobs_to_blobs_directory {
					Self::insert_blob_postgres(blob, &transaction).await?;
				}

				// Insert the objects.
				Self::insert_blob_objects_postgres(blob, &transaction).await?;

				// Commit the transaction.
				transaction
					.commit()
					.await
					.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
			},
		}
		Ok(())
	}

	pub(crate) fn insert_blob_sqlite(
		blob: &Blob,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		// Insert the blob.
		let statement = indoc!(
			"
				insert into blobs (id, reference_count)
				values (?1, 0)
				on conflict (id) do nothing;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the blobs statement"))?;
		let params = rusqlite::params![&blob.id.to_string()];
		statement
			.execute(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(statement);

		// Insert the blob references.
		let statement = indoc!(
			"
				insert into blob_references (id, blob, position, length)
				values (?1, ?2, ?3, ?4)
				on conflict (id) do nothing;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the references statement"))?;
		let blob_id = blob.id.to_string();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			let params =
				rusqlite::params![&blob.id.to_string(), &blob_id, blob.position, blob.length];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			stack.extend(&blob.children);
		}
		drop(statement);

		Ok(())
	}

	pub(crate) async fn insert_blob_postgres(
		blob: &Blob,
		transaction: &postgres::Transaction<'_>,
	) -> tg::Result<()> {
		// Insert the blob.
		let statement = indoc!(
			"
				insert into blobs (id, reference_count)
				values ($1, 0)
				on conflict (id) do nothing;
			"
		);
		transaction
			.execute(statement, &[&blob.id.to_string()])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the blobs statement"))?;

		// Insert the references.
		let mut blobs = Vec::new();
		let mut references = Vec::new();
		let mut stack = vec![blob];
		let blob_id = blob.id.to_string();
		while let Some(blob) = stack.pop() {
			blobs.push(blob.id.to_string());
			references.push((
				blob.id.to_string(),
				blob_id.to_string(),
				blob.position.to_i64().unwrap(),
				blob.length.to_i64().unwrap(),
			));
			stack.extend(&blob.children);
		}
		if !references.is_empty() {
			let (ids, blobs, positions, lengths) =
				references
					.into_iter()
					.collect::<(Vec<_>, Vec<_>, Vec<_>, Vec<_>)>();
			let statement = indoc!(
				"
					insert into blob_references (id, blob, position, length)
					select id, blob, position, length
					from unnest($1::text[], $2::text[], $3::int8[], $4::int8[]) as t (id, blob, position, length)
					on conflict (id) do nothing;
				"
			);
			transaction
				.execute(
					statement,
					&[
						&ids.as_slice(),
						&blobs.as_slice(),
						&positions.as_slice(),
						&lengths.as_slice(),
					],
				)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to execute the references statement")
				})?;
		}

		Ok(())
	}

	pub(crate) fn insert_blob_objects_sqlite(
		blob: &Blob,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		// Prepare a statement for the object children.
		let children_statement = indoc!(
			"
				insert into object_children (object, child)
				values (?1, ?2)
				on conflict (object, child) do nothing;
			"
		);
		let mut children_statement = transaction
			.prepare_cached(children_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare a statement for the objects.
		let objects_statement = indoc!(
			"
				insert into objects (id, complete, count, depth, incomplete_children, size, touched_at, weight)
				values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
				on conflict (id) do update set touched_at = ?7;
			"
		);
		let mut objects_statement = transaction
			.prepare_cached(objects_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Get the current time.
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			// Insert the children.
			for child in &blob.children {
				let params = rusqlite::params![&blob.id.to_string(), &child.id.to_string()];
				children_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				stack.push(child);
			}

			// Insert the object.
			let params = rusqlite::params![
				&blob.id.to_string(),
				true,
				blob.count,
				blob.depth,
				0,
				blob.size,
				&now,
				blob.weight
			];
			objects_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the statements.
		drop(children_statement);
		drop(objects_statement);

		Ok(())
	}

	pub(crate) async fn insert_blob_objects_postgres(
		blob: &Blob,
		transaction: &postgres::Transaction<'_>,
	) -> tg::Result<()> {
		// Collect all the object data and children.
		let mut objects = Vec::new();
		let mut children = Vec::new();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			// Collect object data.
			objects.push((
				blob.id.to_string(),
				blob.count.to_i64().unwrap(),
				blob.depth.to_i64().unwrap(),
				blob.size.to_i64().unwrap(),
				blob.weight.to_i64().unwrap(),
			));

			// Collect children.
			for child in &blob.children {
				children.push((blob.id.to_string(), child.id.to_string()));
				stack.push(child);
			}
		}

		// Get the current time.
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

		// Insert object children using batch insert.
		if !children.is_empty() {
			let (parent_ids, child_ids) = children.into_iter().collect::<(Vec<_>, Vec<_>)>();
			let children_statement = indoc!(
				"
					insert into object_children (object, child)
					select object, child
					from unnest($1::text[], $2::text[]) as t (object, child)
					on conflict (object, child) do nothing;
				"
			);
			transaction
				.execute(
					children_statement,
					&[&parent_ids.as_slice(), &child_ids.as_slice()],
				)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to execute the children statement")
				})?;
		}

		// Insert objects using batch insert
		if !objects.is_empty() {
			let (ids, counts, depths, sizes, weights) =
				objects
					.into_iter()
					.collect::<(Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>)>();
			let statement = indoc!(
				"
					insert into objects (id, complete, count, depth, incomplete_children, size, touched_at, weight)
					select id, 1, count, depth, 0, size, $6, weight
					from unnest($1::text[], $2::int8[], $3::int8[], $4::int8[], $5::int8[]) as t (id, count, depth, size, weight)
					on conflict (id) do update set touched_at = $6;
				"
			);
			transaction
				.execute(
					statement,
					&[
						&ids.as_slice(),
						&counts.as_slice(),
						&depths.as_slice(),
						&sizes.as_slice(),
						&weights.as_slice(),
						&now,
					],
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the objects statement"))?;
		}

		Ok(())
	}

	async fn store_blob(&self, blob: &Blob) -> tg::Result<()> {
		let mut batch = Vec::new();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			if let Some(data) = &blob.data {
				let bytes = data.serialize()?;
				batch.push((blob.id.clone().into(), bytes));
			}
			stack.extend(&blob.children);
		}
		self.store
			.put_batch(&batch)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
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
