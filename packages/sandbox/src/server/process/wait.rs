use {
	crate::server::Server,
	futures::{StreamExt as _, stream},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

impl Server {
	pub async fn wait(&self, index: u64) -> tg::Result<crate::client::wait::Output> {
		let task = self
			.processes
			.get(&index)
			.ok_or_else(|| tg::error!(process = %index, "not found"))?
			.task
			.clone();
		let status = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, process = %index, "the process task panicked"))??;
		let output = crate::client::wait::Output { status };
		Ok(output)
	}

	pub(crate) async fn handle_wait_request(
		&self,
		_request: http::Request<BoxBody>,
		index: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let index: u64 = index
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process index"))?;
		let server = self.clone();
		let stream = stream::once(async move {
			server
				.wait(index)
				.await
				.map(crate::client::wait::Event::Output)
		});
		let stream = stream.map(
			|result: tg::Result<crate::client::wait::Event>| match result {
				Ok(event) => event.try_into(),
				Err(error) => error.try_into(),
			},
		);
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::TEXT_EVENT_STREAM.to_string(),
			)
			.body(BoxBody::with_sse_stream(stream))
			.unwrap();
		Ok(response)
	}
}
