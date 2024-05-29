use crate::Server;
use bytes::Bytes;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use num::ToPrimitive as _;
use std::pin::pin;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tokio::io::AsyncRead;

const MAX_BRANCH_CHILDREN: usize = 1024;
const MIN_LEAF_SIZE: u32 = 4096;
const AVG_LEAF_SIZE: u32 = 16_384;
const MAX_LEAF_SIZE: u32 = 65_536;

impl Server {
	pub async fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::blob::create::Output> {
		// Create the reader.
		let reader = pin!(reader);
		let mut reader = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);

		// Create the leaves.
		let mut children = reader
			.as_stream()
			.map_err(|source| tg::error!(!source, "failed to read from the reader"))
			.and_then(|chunk| async {
				let bytes = Bytes::from(chunk.data);
				let size = bytes.len().to_u64().unwrap();
				let leaf = tg::Leaf::new(bytes);
				leaf.store(self, None).await?;
				leaf.unload();
				Ok::<_, tg::Error>(tg::branch::Child {
					blob: leaf.into(),
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
							let branch = tg::Branch::new(chunk);
							branch.store(self, None).await?;
							branch.unload();
							let child = tg::branch::Child {
								blob: branch.into(),
								size,
							};
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

		// Create the blob.
		let blob = tg::Blob::new(children);
		blob.store(self, None).await?;
		blob.unload();
		let blob = blob.id(self, None).await?;

		// Create the output.
		let output = tg::blob::create::Output { blob };

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
