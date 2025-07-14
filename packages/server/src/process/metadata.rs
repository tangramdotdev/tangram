use crate::Server;
use futures::{FutureExt as _, future};
use tangram_client::{self as tg, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};

impl Server {
	pub async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		if let Some(metadata) = self.try_get_process_metadata_local(id).await? {
			Ok(Some(metadata))
		} else if let Some(metadata) = self.try_get_process_metadata_remote(id).await? {
			Ok(Some(metadata))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_process_metadata_local(
		&self,
		_id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		todo!()
	}

	async fn try_get_process_metadata_remote(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		// Attempt to get the process metadata from the remotes.
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| async move { client.get_process_metadata(id).await }.boxed())
			.collect::<Vec<_>>();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((metadata, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		Ok(Some(metadata))
	}

	pub(crate) async fn handle_get_process_metadata_request<H>(
		handle: &H,
		_request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_process_metadata(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
