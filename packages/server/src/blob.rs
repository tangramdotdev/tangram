use crate::{database, Server};
use futures::{stream, StreamExt, TryStreamExt};
use num::ToPrimitive;
use tangram_client as tg;
use tangram_error::{error, Error, Result};
use tokio::io::AsyncRead;

const MAX_BRANCH_CHILDREN: usize = 1024;

const MIN_LEAF_SIZE: u32 = 4096;
const AVG_LEAF_SIZE: u32 = 16_384;
const MAX_LEAF_SIZE: u32 = 65_536;

impl Server {
	pub async fn create_blob_with_reader(
		&self,
		reader: impl AsyncRead + Unpin,
		txn: &database::Transaction<'_>,
	) -> Result<tg::blob::Id> {
		let mut chunker = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);
		let stream = chunker.as_stream();
		let mut leaves = stream
			.then(|chunk| async {
				let bytes = chunk
					.map_err(|source| error!(!source, "failed to read data"))?
					.data;
				let size = bytes.len().to_u64().unwrap();
				let data = tg::leaf::Data {
					bytes: bytes.into(),
				};
				let id = tg::leaf::Id::new(&data.serialize()?);
				self.put_complete_object_with_transaction(id.clone().into(), data.into(), txn)
					.await?;
				Ok::<_, tangram_error::Error>(tg::branch::child::Data {
					blob: id.into(),
					size,
				})
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Create the tree.
		while leaves.len() > MAX_BRANCH_CHILDREN {
			leaves = stream::iter(leaves)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(async move {
							let size = chunk.iter().fold(0, |acc, data| acc + data.size);

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
