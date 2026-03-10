use {
	crate::server::Server,
	bytes::Bytes,
	futures::stream,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn wait(
		&self,
		arg: crate::client::wait::Arg,
	) -> tg::Result<crate::client::wait::Output> {
		#[cfg(target_os = "linux")]
		{
			use std::sync::Arc;
			loop {
				let child = self
					.processes
					.get_mut(&arg.id)
					.ok_or_else(|| tg::error!(process = %arg.id, "not found"))?;
				if let Some(status) = child.status {
					return Ok(crate::client::wait::Output { status });
				}
				let notify = Arc::clone(&child.notify);
				drop(child);
				notify.notified().await;
			}
		}
		#[cfg(target_os = "macos")]
		{
			use num::ToPrimitive as _;
			use std::os::unix::process::ExitStatusExt as _;
			let (_, mut child) = self
				.processes
				.remove(&arg.id)
				.ok_or_else(|| tg::error!(process = %arg.id, "not found"))?;
			let status = child
				.child
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "failed to wait the process"))?;
			let status = status
				.code()
				.or(status.signal())
				.map_or(128, |code| code.to_u8().unwrap());
			Ok(crate::client::wait::Output { status })
		}
	}

	pub(crate) async fn handle_wait(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;
		let server = self.clone();
		let stream = stream::once(async move {
			let result = server.wait(arg).await;
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
