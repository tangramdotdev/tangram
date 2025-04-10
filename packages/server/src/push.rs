use crate::Server;
use futures::{Stream, StreamExt as _, TryFutureExt as _, stream::FuturesUnordered};
use std::{pin::pin, time::Duration};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext as _};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let remote = self.get_remote_client(arg.remote.clone()).await?;
		Self::push_or_pull(self, &remote, &arg).await
	}

	pub(crate) async fn push_or_pull<S, D>(
		src: &S,
		dst: &D,
		arg: &tg::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + use<S, D>>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		// Create the progress handle and add the indicators.
		let progress = crate::progress::Handle::new();
		if arg.items.iter().any(Either::is_left) {
			progress.start(
				"pull-processes".to_owned(),
				"processes".to_owned(),
				tg::progress::IndicatorFormat::Normal,
				Some(0),
				None,
			);
		}
		progress.start(
			"pull-objects".to_owned(),
			"objects".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"pull-bytes".to_owned(),
			"bytes".to_owned(),
			tg::progress::IndicatorFormat::Bytes,
			Some(0),
			None,
		);

		// Spawn a task to set the indicator totals as soon as they are ready.
		let indicator_total_task = tokio::spawn({
			let src = src.clone();
			let progress = progress.clone();
			let arg = arg.clone();
			async move {
				let mut metadata_futures = arg
					.items
					.iter()
					.map(|item| {
						let src = src.clone();
						async move {
							loop {
								match item {
									tangram_either::Either::Left(process) => {
										let Some(metadata) = src
											.try_get_process_metadata(process)
											.await
											.map_err(|source| {
												tg::error!(!source, "failed to get the process")
											})?
										else {
											return Err(tg::error!("failed to get the process"));
										};
										let mut complete = metadata.count.is_some();
										if arg.commands {
											complete = complete
												&& metadata.commands_count.is_some()
												&& metadata.commands_weight.is_some();
										}
										if arg.outputs {
											complete = complete
												&& metadata.outputs_count.is_some()
												&& metadata.outputs_weight.is_some();
										}
										if complete {
											break Ok::<_, tg::Error>(Either::Left(metadata));
										}
									},
									tangram_either::Either::Right(id) => {
										let metadata =
											src.try_get_object_metadata(id).await?.ok_or_else(
												|| tg::error!("expected the metadata to be set"),
											)?;
										if metadata.count.is_some() && metadata.weight.is_some() {
											break Ok::<_, tg::Error>(Either::Right(metadata));
										}
									},
								}
								tokio::time::sleep(Duration::from_secs(1)).await;
							}
						}
					})
					.collect::<FuturesUnordered<_>>();
				let mut total_processes: u64 = 0;
				let mut total_objects: u64 = 0;
				let mut total_bytes: u64 = 0;
				while let Some(Ok(metadata)) = metadata_futures.next().await {
					match metadata {
						Either::Left(metadata) => {
							if let Some(count) = metadata.count {
								total_processes += count;
								progress.set_total("pull-processes", total_processes);
							}
							if arg.commands {
								if let Some(commands_count) = metadata.commands_count {
									total_objects += commands_count;
								}
								if let Some(commands_weight) = metadata.commands_weight {
									total_bytes += commands_weight;
								}
							}
							if arg.outputs {
								if let Some(outputs_count) = metadata.outputs_count {
									total_objects += outputs_count;
								}
								if let Some(outputs_weight) = metadata.outputs_weight {
									total_bytes += outputs_weight;
								}
							}
							progress.set_total("pull-objects", total_objects);
							progress.set_total("pull-bytes", total_bytes);
						},
						Either::Right(metadata) => {
							if let Some(count) = metadata.count {
								total_objects += count;
								progress.set_total("pull-objects", total_objects);
							}
							if let Some(weight) = metadata.weight {
								total_bytes += weight;
								progress.set_total("pull-bytes", total_bytes);
							}
						},
					}
				}
			}
		});
		let indicator_total_task_abort_handle = AbortOnDropHandle::new(indicator_total_task);

		// Spawn the task.
		let task = tokio::spawn({
			let progress = progress.clone();
			let arg = arg.clone();
			let src = src.clone();
			let dst = dst.clone();
			Self::push_or_pull_task(arg, progress, src, dst).inspect_err(|error| {
				tracing::error!(?error, "the push or pull task failed");
			})
		});

		// Create the stream.
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress
			.stream()
			.attach(abort_handle)
			.attach(indicator_total_task_abort_handle);

		Ok(stream)
	}

	async fn push_or_pull_task<S, D>(
		arg: tg::push::Arg,
		progress: crate::progress::Handle<()>,
		src: S,
		dst: D,
	) -> tg::Result<()>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		loop {
			// Create channels to send events.
			let (export_item_sender, export_item_receiver) = tokio::sync::mpsc::channel(1024);
			let (import_event_sender, import_event_receiver) = tokio::sync::mpsc::channel(1024);

			// Start the export.
			let export_arg = tg::export::Arg {
				commands: arg.commands,
				items: arg.items.clone(),
				outputs: arg.outputs,
				recursive: arg.recursive,
				remote: None,
			};
			let import_event_stream = ReceiverStream::new(import_event_receiver);
			let export_event_stream = src
				.export(export_arg, import_event_stream.boxed())
				.await
				.map_err(|source| tg::error!(!source, "failed to create an export stream"))?;

			// Start the import.
			let import_arg = tg::import::Arg {
				items: arg.items.clone(),
				remote: None,
			};
			let export_item_stream = ReceiverStream::new(export_item_receiver);
			let import_event_stream = dst
				.import(import_arg, export_item_stream.boxed())
				.await
				.map_err(|source| tg::error!(!source, "failed to create a import stream"))?;

			// Create the export future.
			let export_future = {
				let progress = progress.clone();
				async move {
					let mut export_event_stream = pin!(export_event_stream);
					while let Some(result) = export_event_stream.next().await {
						match result {
							Ok(tg::export::Event::Item(item)) => {
								let result = export_item_sender.send(Ok(item)).await;
								if let Err(error) = result {
									progress
										.error(tg::error!(!error, "failed to send export item"));
									break;
								}
							},
							Ok(tg::export::Event::Complete(complete)) => match complete {
								tg::export::Complete::Process(process_complete) => {
									if let Some(processes) = process_complete.count {
										progress.increment("pull-processes", processes);
									}
									let mut objects = 0;
									let mut bytes = 0;
									if let Some(commands_count) = process_complete.commands_count {
										objects += commands_count;
									}
									if let Some(commands_weight) = process_complete.commands_weight
									{
										bytes += commands_weight;
									}
									if let Some(outputs_count) = process_complete.outputs_count {
										objects += outputs_count;
									}
									if let Some(outputs_weight) = process_complete.outputs_weight {
										bytes += outputs_weight;
									}
									progress.increment("pull-objects", objects);
									progress.increment("pull-bytes", bytes);
								},
								tg::export::Complete::Object(object_complete) => {
									if let Some(count) = object_complete.count {
										progress.increment("pull-objects", count);
									}
									if let Some(weight) = object_complete.weight {
										progress.increment("pull-bytes", weight);
									}
								},
							},
							Ok(tg::export::Event::End) => return true,
							Err(error) => {
								progress.error(error);
							},
						}
					}
					false
				}
			};

			// Create the import future.
			let import_future = {
				let progress = progress.clone();
				async move {
					let mut import_event_stream = pin!(import_event_stream);
					while let Some(result) = import_event_stream.next().await {
						match result {
							Ok(tg::import::Event::Complete(complete)) => {
								import_event_sender.send(Ok(complete)).await.ok();
							},
							Ok(tg::import::Event::Progress(import_progress)) => {
								if let Some(processes) = import_progress.processes {
									progress.increment("pull-processes", processes);
								}
								progress.increment("pull-objects", import_progress.objects);
								progress.increment("pull-bytes", import_progress.bytes);
							},
							Ok(tg::import::Event::End) => return true,
							Err(error) => progress.error(error),
						}
					}
					false
				}
			};

			// Join the futures.
			let (export_finished, import_finished) = futures::join!(export_future, import_future);
			let finished = export_finished && import_finished;

			// If the export and import completed, then break.
			if finished {
				break;
			}
		}

		progress.finish("pull-processes");
		progress.finish("pull-objects");
		progress.finish("pull-bytes");
		progress.output(());

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_push_request<H>(
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

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.push(arg).await?;

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
