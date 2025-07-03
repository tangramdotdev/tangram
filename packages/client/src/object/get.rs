use crate as tg;
use bytes::Bytes;
use futures::stream::{FuturesUnordered, TryStreamExt as _};
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug)]
pub struct Output {
	pub bytes: Bytes,
}

pub type BatchOutput = Vec<BatchOutputItem>;

#[derive(Clone, Debug)]
pub struct BatchOutputItem {
	pub id: tg::object::Id,
	pub bytes: Bytes,
}

impl tg::Client {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let method = http::Method::GET;
		let uri = format!("/objects/{id}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let bytes = response.bytes().await?;
		let output = tg::object::get::Output { bytes };
		Ok(Some(output))
	}

	pub async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<tg::object::get::BatchOutput> {
		let results = ids
			.iter()
			.map(|id| async move {
				match self.try_get_object(id).await? {
					Some(output) => Ok::<_, tg::Error>(Some(tg::object::get::BatchOutputItem {
						id: id.clone(),
						bytes: output.bytes,
					})),
					None => Ok(None),
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.flatten()
			.collect();

		Ok(results)
	}
}
