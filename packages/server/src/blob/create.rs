use crate::{
	database::Transaction,
	util::http::{full, Incoming, Outgoing},
	Http, Server,
};
use bytes::Bytes;
use futures::{future, stream, FutureExt as _, StreamExt as _, TryStreamExt as _};
use http_body_util::BodyStream;
use num::ToPrimitive;
use std::pin::pin;
use tangram_client as tg;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

const MAX_BRANCH_CHILDREN: usize = 1024;
const MIN_LEAF_SIZE: u32 = 4096;
const AVG_LEAF_SIZE: u32 = 16_384;
const MAX_LEAF_SIZE: u32 = 65_536;

impl Server {
	pub async fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		transaction: Option<&Transaction<'_>>,
	) -> tg::Result<tg::blob::Id> {
		let reader = pin!(reader);

		// Create the leaves.
		let mut chunker = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			MIN_LEAF_SIZE,
			AVG_LEAF_SIZE,
			MAX_LEAF_SIZE,
		);
		let mut children = chunker
			.as_stream()
			.map_err(|source| tg::error!(!source, "failed to read from the reader"))
			.and_then(|chunk| async {
				let bytes = Bytes::from(chunk.data);
				let size = bytes.len().to_u64().unwrap();
				let id = tg::leaf::Id::new(&bytes);
				let arg = tg::object::PutArg {
					bytes,
					count: None,
					weight: None,
				};
				self.put_object(&id.clone().into(), arg, transaction)
					.await?;
				Ok::<_, tg::Error>(tg::branch::child::Data {
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
							let arg = tg::object::PutArg {
								bytes,
								count: None,
								weight: None,
							};
							self.put_object(&id.clone().into(), arg, transaction)
								.await?;
							let blob = id.into();
							let child = tg::branch::child::Data { blob, size };
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
		let data = tg::branch::Data { children };
		let bytes = data.serialize()?;
		let id = tg::branch::Id::new(&bytes);
		let arg = tg::object::PutArg {
			bytes,
			count: None,
			weight: None,
		};
		self.put_object(&id.clone().into(), arg, transaction)
			.await?;

		Ok(id.into())
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_create_blob_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the reader.
		let reader = StreamReader::new(
			BodyStream::new(request.into_body())
				.try_filter_map(|frame| future::ok(frame.into_data().ok()))
				.map_err(std::io::Error::other),
		);

		// Create the blob.
		let output = self.inner.tg.create_blob(reader, None).boxed().await?;

		// Create the response.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the response"))?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}
}
