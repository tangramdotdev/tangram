use super::Server;
use futures::{Stream, StreamExt as _};
use num::ToPrimitive as _;
use std::time::Duration;
use tangram_client as tg;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tokio_util::task::AbortOnDropHandle;

struct InnerOutput {
	cache_entries: Vec<tg::artifact::Id>,
	objects: Vec<tg::object::Id>,
	pipes: Vec<tg::pipe::Id>,
	processes: Vec<tg::process::Id>,
	ptys: Vec<tg::pty::Id>,
}

struct Count {
	cache_entries: u64,
	objects: u64,
	pipes: u64,
	processes: u64,
	ptys: u64,
}

impl Server {
	pub async fn clean(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let progress = crate::progress::Handle::new();

		let task = AbortOnDropHandle::new(tokio::spawn({
			let progress = progress.clone();
			let server = self.clone();
			async move {
				// Clean the temporary directory.
				crate::util::fs::remove(server.temp_path())
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to remove the temporary directory")
					})?;
				tokio::fs::create_dir_all(server.temp_path())
					.await
					.map_err(|error| {
						tg::error!(source = error, "failed to recreate the temporary directory")
					})?;

				let count = server.count_items().await?;

				// Clean until there are no more items to remove.
				if count.cache_entries > 0 {
					progress.start(
						"cache".into(),
						"cache entries".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.cache_entries),
					);
				}
				if count.objects > 0 {
					progress.start(
						"objects".into(),
						"objects".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.objects),
					);
				}
				if count.processes > 0 {
					progress.start(
						"processes".into(),
						"processes".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.processes),
					);
				}
				if count.pipes > 0 {
					progress.start(
						"pipes".into(),
						"pipes".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.pipes),
					);
				}
				if count.ptys > 0 {
					progress.start(
						"ptys".into(),
						"ptys".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.ptys),
					);
				}

				loop {
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					let ttl = Duration::from_secs(0);
					let batch_size = server
						.config
						.cleaner
						.as_ref()
						.map_or(1024, |config| config.batch_size);
					let output = match server.cleaner_task_inner(now, ttl, batch_size).await {
						Ok(output) => output,
						Err(error) => {
							progress.error(error);
							break;
						},
					};
					progress.increment("cache", output.cache_entries.len().to_u64().unwrap());
					progress.increment("objects", output.objects.len().to_u64().unwrap());
					progress.increment("processes", output.processes.len().to_u64().unwrap());
					progress.increment("pipes", output.pipes.len().to_u64().unwrap());
					progress.increment("ptys", output.ptys.len().to_u64().unwrap());
					let n =
						output.processes.len() + output.objects.len() + output.cache_entries.len();
					if n == 0 {
						break;
					}
				}
				progress.output(());
				Ok::<_, tg::Error>(())
			}
		}));
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	pub(crate) async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		loop {
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			let ttl = config.ttl;
			let batch_size = config.batch_size;
			let result = self.cleaner_task_inner(now, ttl, batch_size).await;
			match result {
				Ok(output) => {
					let n = output.processes.len()
						+ output.objects.len()
						+ output.cache_entries.len()
						+ output.pipes.len()
						+ output.ptys.len();
					if n == 0 {
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				},
				Err(error) => {
					tracing::error!(?error, "failed to clean");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn count_items(&self) -> tg::Result<Count> {
		todo!()
	}

	async fn cleaner_task_inner(
		&self,
		_now: i64,
		_ttl: std::time::Duration,
		_batch_size: usize,
	) -> tg::Result<InnerOutput> {
		todo!()
	}

	pub(crate) async fn handle_server_clean_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the stream.
		let stream = handle.clean().await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
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
