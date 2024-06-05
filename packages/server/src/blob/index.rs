use super::{AVG_LEAF_SIZE, MAX_BRANCH_CHILDREN, MAX_LEAF_SIZE, MIN_LEAF_SIZE};
use crate::Server;
use bytes::Bytes;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive;
use std::sync::{
	atomic::{AtomicU64, Ordering},
	Arc,
};
use tangram_client as tg;
use tangram_database::{self as db, Connection as _, Database as _, Query as _, Transaction};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_index_blob(
		&self,
		blob: tg::blob::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// Try to open the blob file if it exists.
		let path = self.blobs_path().join(blob.to_string());
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let file = match tokio::fs::File::open(&path).await {
			Ok(file) => file,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				eprintln!("~/.tangram/blobs/{blob} does not exist");
				return Ok(None);
			},
			Err(source) => return Err(tg::error!(!source, %blob, "failed to open blob file")),
		};

		// Create the reader.
		let mut reader =
			fastcdc::v2020::AsyncStreamCDC::new(file, MIN_LEAF_SIZE, AVG_LEAF_SIZE, MAX_LEAF_SIZE);

		// Create a database transaction.
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to acquire a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to acquire a transaction"))?;
		let p = transaction.p();

		// Create the leaves.
		let total_weight = Arc::new(AtomicU64::new(0));
		let mut children = reader
			.as_stream()
			.map_err(|source| tg::error!(!source, "failed to read from the reader"))
			.and_then(|chunk| async {
				// Create the leaf data.
				let bytes = Bytes::from(chunk.data);
				let size = bytes.len().to_u64().unwrap();
				let data = tg::leaf::Data { bytes };
				let id = tg::leaf::Id::new(&data.serialize()?);

				// Put the data into the database.
				let statement: String = formatdoc!(
					"
							insert into objects (id, bytes, touched_at, count, weight)
							values ({p}1, {p}2, {p}3, {p}4, {p}5)
							on conflict (id) do update set touched_at = {p}3;
						"
				);
				let bytes = data.serialize()?;
				let weight = size;
				let count = weight;
				let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
				let params = db::params![id.clone(), bytes, now, count, weight];
				transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				// Create the child data.
				let blob = id.into();
				let child = tg::branch::child::Data { blob, size };

				total_weight.fetch_add(weight, Ordering::Relaxed);

				// Update the total weight.
				Ok::<_, tg::Error>(child)
			})
			.try_collect::<Vec<_>>()
			.await?;
		drop(permit);

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
							let id = tg::branch::Id::new(&data.serialize()?);

							// Put the data into the database.
							let statement = formatdoc!(
								"
									insert into objects (id, bytes, touched_at, count, weight)
									values ({p}1, {p}2, {p}3, {p}4, {p}5)
									on conflict (id) do update set touched_at = {p}3;
								"
							);
							let bytes = data.serialize()?;
							let weight = bytes.len().to_u64().unwrap() + size;
							let count = weight;

							let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
							let params = db::params![id.clone(), bytes, now, count, weight];
							transaction
								.execute(statement, params)
								.await
								.map_err(|source| {
									tg::error!(!source, "failed to execute the statement")
								})?;

							// Create the child data.
							let blob = id.into();
							let child = tg::branch::child::Data { blob, size };

							total_weight.fetch_add(weight - size, Ordering::Relaxed);

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

		// Get the id.
		let (id, bytes) = match children.len() {
			0 => {
				let data = tg::leaf::Data {
					bytes: Bytes::default(),
				};
				let bytes = data.serialize()?;
				let id = tg::leaf::Id::new(&bytes).into();
				(id, bytes)
			},
			1 => {
				let id = children[0].blob.clone();
				#[derive(serde::Deserialize)]
				struct Row {
					bytes: Bytes,
				}
				let statement = formatdoc!(
					"
						select bytes from objects where id = {p}1;
					"
				);
				let params = db::params![id.clone()];
				let row = transaction
					.query_one_into::<Row>(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to get leaf bytes"))?;
				let bytes = row.bytes;
				(id, bytes)
			},
			_ => {
				let data = tg::branch::Data { children };
				let bytes = data.serialize()?;
				let id = tg::branch::Id::new(&bytes).into();
				(id, bytes)
			},
		};

		// Validate the ID.
		if id != blob {
			transaction
				.rollback()
				.await
				.map_err(|source| tg::error!(!source, "failed to rollback transaction"))?;
			return Err(tg::error!(%expected = blob, %blob = id, "invalid blob ID"));
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Spawn the indexing task.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				let subject = "objects.index".to_owned();
				let payload = id.to_string().into();
				server.messenger.publish(subject, payload).await.ok();
			}
		});

		// Create the output.
		let count = total_weight.load(Ordering::Relaxed);
		let weight = count;
		let metadata = tg::object::Metadata {
			count: Some(count),
			weight: Some(weight),
		};
		Ok(Some(tg::object::get::Output { bytes, metadata }))
	}
}
