use crate::{database, Server};
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use num::ToPrimitive;
use tangram_client as tg;
use tangram_error::{Error, Result, WrapErr};
use tokio::io::{AsyncRead, AsyncReadExt};

const MAX_BRANCH_CHILDREN: usize = 1024;

const MAX_LEAF_SIZE: usize = 262_144;

impl Server {
	/// Create a blob from a reader in a single transaction.
	pub async fn create_blob_with_reader(
		&self,
		reader: impl AsyncRead + Unpin,
	) -> Result<tg::Blob> {
		let mut connection = self.inner.database.get().await?;
		let txn = connection.transaction().await?;
		let id = self
			.create_blob_with_reader_and_transaction(reader, &txn)
			.await?;
		txn.commit().await?;
		Ok(tg::Blob::with_id(id))
	}

	/// Create a blob from a reader using an existing transaction.
	pub async fn create_blob_with_reader_and_transaction(
		&self,
		mut reader: impl AsyncRead + Unpin,
		txn: &database::Transaction<'_>,
	) -> Result<tg::blob::Id> {
		let mut leaves = Vec::new();
		let mut bytes = vec![0u8; MAX_LEAF_SIZE];
		loop {
			// Read up to `MAX_LEAF_BLOCK_DATA_SIZE` bytes from the reader.
			let mut position = 0;
			loop {
				let n = reader
					.read(&mut bytes[position..])
					.await
					.wrap_err("Failed to read from the reader.")?;
				position += n;
				if n == 0 || position == bytes.len() {
					break;
				}
			}
			if position == 0 {
				break;
			}
			let size = position.to_u64().unwrap();

			// Create the leaf.
			let bytes = Bytes::copy_from_slice(&bytes[..position]);
			let data = tg::leaf::Data { bytes };
			let id = tg::leaf::Id::new(&data.serialize()?);
			self.put_complete_object_with_transaction(id.clone().into(), data.into(), txn)
				.await?;

			// Add to the tree.
			leaves.push(tg::branch::child::Data {
				blob: id.into(),
				size,
			});
		}

		// Create the tree.
		while leaves.len() > MAX_BRANCH_CHILDREN {
			leaves = stream::iter(leaves)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(async move {
							let size = chunk.len().to_u64().unwrap();
							let data = tg::branch::Data { children: chunk };
							let id = tg::branch::Id::new(&data.serialize()?);
							self.put_complete_object_with_transaction(
								id.clone().into(),
								data.into(),
								txn,
							)
							.await?;
							Ok::<_, Error>(tg::branch::child::Data {
								blob: id.into(),
								size,
							})
						})
						.boxed_local()
					} else {
						stream::iter(chunk.into_iter().map(Result::Ok)).boxed_local()
					}
				})
				.try_collect()
				.await?;
		}

		// Create the blob.
		let data = tg::branch::Data { children: leaves };
		let id = tg::branch::Id::new(&data.serialize()?);
		self.put_complete_object_with_transaction(id.clone().into(), data.into(), txn)
			.await?;
		Ok(id.into())
	}
}
