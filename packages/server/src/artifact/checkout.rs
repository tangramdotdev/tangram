use crate::Server;
use futures::{Stream, StreamExt as _};
use std::pin::pin;
use tangram_client as tg;
use tangram_futures::{stream::TryStreamExt as _, task::Task};
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};

mod external;
mod internal;
mod lockfile;
#[cfg(test)]
mod tests;

impl Server {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>,
	> {
		// Create the progress handle.
		let progress = crate::progress::Handle::new();

		// Spawn the task.
		let task = Task::spawn(|_| {
			let server = self.clone();
			let id = id.clone();
			let arg = arg.clone();
			let progress = progress.clone();
			async move { server.check_out_artifact_task(id, arg, &progress).await }
		});

		// Spawn the progress task.
		tokio::spawn({
			let progress = progress.clone();
			async move {
				match task.wait().await {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(source) => {
						progress.error(tg::error!(!source, "the task panicked"));
					},
				};
			}
		});

		// Create the stream.
		let stream = progress.stream();

		Ok(stream)
	}

	async fn check_out_artifact_task(
		&self,
		artifact: tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<tg::artifact::checkout::Output> {
		match arg.path.clone() {
			None => self.check_out_artifact_internal(artifact, progress).await,
			Some(path) => {
				self.check_out_artifact_external(artifact, arg, path, progress)
					.await
			},
		}
	}
}

impl Server {
	pub(crate) async fn handle_check_out_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.check_out_artifact(&id, arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None => {
				pin!(stream)
					.try_last()
					.await?
					.and_then(|event| event.try_unwrap_output().ok())
					.ok_or_else(|| tg::error!("stream ended without output"))?;
				(None, Outgoing::empty())
			},

			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Outgoing::sse(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
