use crate::Server;
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database as db;
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
		transaction: &db::Transaction<'_>,
	) -> Result<tg::blob::Id> {
		// Create the leaves.
		let mut chunker = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);
		let mut children = chunker
			.as_stream()
			.map_err(|source| error!(!source, "failed to read"))
			.and_then(|chunk| async {
				let bytes = Bytes::from(chunk.data);
				let size = bytes.len().to_u64().unwrap();
				let id = tg::leaf::Id::new(&bytes);
				self.put_object_with_transaction(id.clone().into(), bytes, transaction)
					.await?;
				Ok::<_, Error>(tg::branch::child::Data {
					blob: id.into(),
					size,
				})
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Create the tree.
		while children.len() > MAX_BRANCH_CHILDREN {
			children = stream::iter(children)
				.chunks(MAX_BRANCH_CHILDREN)
				.flat_map(|chunk| {
					if chunk.len() == MAX_BRANCH_CHILDREN {
						stream::once(async move {
							let size = chunk.iter().map(|blob| blob.size).sum();
							let data = tg::branch::Data { children: chunk };
							let bytes = data.serialize()?;
							let id = tg::branch::Id::new(&bytes);
							self.put_object_with_transaction(id.clone().into(), bytes, transaction)
								.await?;
							let blob = id.into();
							let child = tg::branch::child::Data { blob, size };
							Ok::<_, Error>(child)
						})
						.left_stream()
					} else {
						stream::iter(chunk.into_iter().map(Ok)).right_stream()
					}
				})
				.try_collect()
				.await?;
		}

		// Create the blob.
		let data = tg::branch::Data { children };
		let bytes = data.serialize()?;
		let id = tg::branch::Id::new(&bytes);
		self.put_object_with_transaction(id.clone().into(), bytes, transaction)
			.await?;

		Ok(id.into())
	}
}
