use {
	crate::server::Server,
	bytes::Bytes,
	futures::stream,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, response::builder::Ext as _},
};

impl Server {
	pub async fn wait(&self, id: tg::process::Id) -> tg::Result<crate::client::wait::Output> {
		use std::sync::Arc;
		loop {
			let child = self
				.processes
				.get_mut(&id)
				.ok_or_else(|| tg::error!(process = %id, "not found"))?;
			if let Some(status) = &child.status {
				return Ok(crate::client::wait::Output {
					status: status.clone()?,
				});
			}
			let notify = Arc::clone(&child.notify);
			drop(child);
			notify.notified().await;
		}
	}

	pub(crate) async fn handle_wait_request(
		&self,
		_request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id: tg::process::Id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let server = self.clone();
		let stream = stream::once(async move {
			let result = server.wait(id).await;
			let bytes = match result {
				Ok(output) => serde_json::to_vec(&output).unwrap_or_default(),
				Err(error) => error
					.state()
					.object()
					.and_then(|object| {
						serde_json::to_vec(&object.unwrap_error_ref().to_data()).ok()
					})
					.unwrap_or_default(),
			};
			Ok::<_, std::io::Error>(Bytes::from(bytes))
		});
		let response = http::Response::builder().data_stream(stream).unwrap();
		Ok(response)
	}
}
