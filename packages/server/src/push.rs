use crate::Server;
use futures::{Stream, StreamExt as _};
use std::pin::pin;
use tangram_client as tg;
use tangram_futures::stream::Ext;
use tangram_http::{request::Ext as _, Body};
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
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static + use<S, D>,
	>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		// Create the progress handle and add the indicators.
		let progress = crate::progress::Handle::new();
		progress.start(
			"objects".to_owned(),
			"objects".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"bytes".to_owned(),
			"bytes".to_owned(),
			tg::progress::IndicatorFormat::Bytes,
			Some(0),
			None,
		);

		let (export_item_sender, export_item_receiver) = tokio::sync::mpsc::channel(1024);
		let (import_event_sender, import_event_receiver) = tokio::sync::mpsc::channel(1024);
		let export_arg = tg::export::Arg {
			commands: arg.commands,
			items: arg.items.clone(),
			logs: arg.logs,
			outputs: arg.outputs,
			recursive: arg.recursive,
			remote: None,
		};
		let import_event_stream = ReceiverStream::new(import_event_receiver);
		let export_event_stream = src.export(export_arg, import_event_stream.boxed()).await?;
		let import_arg = tg::import::Arg {
			items: arg.items.clone(),
			remote: None,
		};
		let export_item_stream = ReceiverStream::new(export_item_receiver);
		let import_event_stream = dst.import(import_arg, export_item_stream.boxed()).await?;
		let task = tokio::spawn({
			let progress = progress.clone();
			async move {
				let export_future = {
					let progress = progress.clone();
					async move {
						let mut export_event_stream = pin!(export_event_stream);
						while let Some(result) = export_event_stream.next().await {
							match result {
								Ok(tg::export::Event::Item(item)) => {
									let result = export_item_sender.send(Ok(item)).await;
									if let Err(error) = result {
										progress.error(tg::error!(
											!error,
											"failed to send export item"
										));
										break;
									}
								},
								Ok(tg::export::Event::Complete(complete)) => match complete {
									tg::export::Complete::Process(process_complete) => {
										if progress.has_indicator("processes") {
											progress.increment("processes", 1);
										} else {
											progress.start(
												"processes".to_owned(),
												"processes".to_owned(),
												tg::progress::IndicatorFormat::Normal,
												Some(1),
												None,
											);
										}
										let mut count = 0;
										let mut weight = 0;
										if let Some(commands_count) =
											process_complete.commands_count
										{
											count += commands_count;
										}
										if let Some(commands_weight) =
											process_complete.commands_weight
										{
											weight += commands_weight;
										}
										if let Some(logs_count) = process_complete.logs_count {
											count += logs_count;
										}
										if let Some(logs_weight) = process_complete.logs_weight {
											weight += logs_weight;
										}
										if let Some(outputs_count) = process_complete.outputs_count
										{
											count += outputs_count;
										}
										if let Some(outputs_weight) =
											process_complete.outputs_weight
										{
											weight += outputs_weight;
										}

										progress.increment("objects", count);
										progress.increment("weight", weight);
									},
									tg::export::Complete::Object(object_complete) => {
										if let Some(count) = object_complete.count {
											progress.increment("objects", count);
										}
										if let Some(weight) = object_complete.weight {
											progress.increment("bytes", weight);
										}
									},
								},
								Err(error) => {
									progress.error(error);
								},
							}
						}
					}
				};
				let import_future = {
					let progress = progress.clone();
					async move {
						let mut import_event_stream = pin!(import_event_stream);
						while let Some(result) = import_event_stream.next().await {
							match result {
								Ok(tg::import::Event::Complete(complete)) => {
									// Try to send the import event to the exporter. Report if this fails, but do not abort.
									let result = import_event_sender.send(Ok(complete)).await;
									if let Err(error) = result {
										progress.error(tg::error!(
											!error,
											"failed to send import complete to exporter"
										));
									}
								},
								Ok(tg::import::Event::Progress(import_progress)) => {
									if import_progress.processes > 0 {
										if progress.has_indicator("processes") {
											progress.set("processes", import_progress.processes);
										} else {
											progress.start(
												"processes".to_owned(),
												"processes".to_owned(),
												tg::progress::IndicatorFormat::Normal,
												Some(import_progress.processes),
												None,
											);
										}
									}
									progress.set("objects", import_progress.objects);
									progress.set("bytes", import_progress.bytes);
								},
								Err(error) => progress.error(error),
							}
						}
					}
				};
				futures::join!(export_future, import_future);
				progress.output(());
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let progress_stream = progress.stream().attach(abort_handle);
		Ok(progress_stream)
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
