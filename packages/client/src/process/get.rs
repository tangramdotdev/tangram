use crate as tg;
use futures::stream::{FuturesUnordered, TryStreamExt as _};
use tangram_http::{request::builder::Ext as _, response::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(flatten)]
	pub data: tg::process::Data,
}

pub type BatchOutput = Vec<BatchOutputItem>;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct BatchOutputItem {
	pub id: tg::process::Id,
	#[serde(flatten)]
	pub data: tg::process::Data,
}

impl Output {
	pub fn objects(&self) -> Vec<tg::object::Id> {
		self.data.objects()
	}
}

impl tg::Client {
	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let method = http::Method::GET;
		let uri = format!("/processes/{id}");
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
		let output = response.json().await?;
		Ok(Some(output))
	}
}

impl tg::Client {
	pub async fn try_get_process_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<tg::process::get::BatchOutput> {
		let results = ids
			.iter()
			.map(|id| async move {
				match self.try_get_process(id).await? {
					Some(output) => Ok::<_, tg::Error>(Some(tg::process::get::BatchOutputItem {
						id: id.clone(),
						data: output.data,
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
