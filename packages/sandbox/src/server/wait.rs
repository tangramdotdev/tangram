use {
	crate::server::Server,
	futures::{StreamExt as _, stream},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

impl Server {
	pub async fn wait(&self, id: tg::process::Id) -> tg::Result<crate::client::wait::Output> {
		let task = self
			.processes
			.get(&id)
			.ok_or_else(|| tg::error!(process = %id, "not found"))?
			.task
			.clone();
		let status = task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, process = %id, "the process task panicked"))??;
		let output = crate::client::wait::Output { status };
		Ok(output)
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
			server
				.wait(id)
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
