use crate::{utils::serde::is_false, Server};
use futures::{stream::FuturesUnordered, Stream, StreamExt as _};
use indoc::formatdoc;
use std::{pin::pin, time::Duration};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
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
		// let progress = crate::progress::Handle::new();
		// progress.start(
		// 	"processes".to_owned(),
		// 	"processes".to_owned(),
		// 	tg::progress::IndicatorFormat::Normal,
		// 	Some(0),
		// 	None,
		// );
		// progress.start(
		// 	"objects".to_owned(),
		// 	"objects".to_owned(),
		// 	tg::progress::IndicatorFormat::Normal,
		// 	Some(0),
		// 	None,
		// );
		// progress.start(
		// 	"bytes".to_owned(),
		// 	"bytes".to_owned(),
		// 	tg::progress::IndicatorFormat::Bytes,
		// 	Some(0),
		// 	None,
		// );
		// Spawn a task to set the indicator totals as soon as they are ready.
		let indicator_total_task = tokio::spawn({
			let server = src.clone();
			let progress = progress.clone();
			let arg = arg.clone();
			async move {
				// FIXME - where does this belong?
				// server
				// 	.set_push_progress_indicator_totals(&arg, &progress)
				// 	.await;
			}
		});
		let indicator_total_task_abort_handle = AbortOnDropHandle::new(indicator_total_task);

		let (export_item_sender, export_item_receiver) = tokio::sync::mpsc::channel(1024);
		let (progress_event_sender, progress_event_receiver) = tokio::sync::mpsc::channel(1024);
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
		let task = tokio::spawn(async move {
			let export_future = {
				let progress_event_sender = progress_event_sender.clone();
				async move {
					let mut export_event_stream = pin!(export_event_stream);
					while let Some(result) = export_event_stream.next().await {
						match result {
							Ok(tg::export::Event::Item(item)) => {
								let result = export_item_sender.send(Ok(item)).await;
								if let Err(error) = result {
									progress_event_sender
										.send(Err(tg::error!(!error, "failed to send export item")))
										.await
										.ok();
									break;
								}
							},
							Ok(tg::export::Event::Complete(complete)) => {
								// TODO - send a progress event noting what the exporter skipped.
								progress_event_sender.send(Ok(event)).await.ok();
							},
							Err(error) => {
								progress_event_sender.send(Err(error)).await.ok();
							},
						}
					}
				}
			};
			let import_future = {
				let progress_event_sender = progress_event_sender.clone();
				async move {
					let mut import_event_stream = pin!(import_event_stream);
					while let Some(result) = import_event_stream.next().await {
						if let Err(error) = &result {
							progress_event_sender.send(Err(error.clone())).await.ok();
						}

						// TODO Filter these results. Complete events go to the sender, Progress events need a progress event sent.
						// import_event_sender.send(result).await.ok();
					}
				}
			};
			futures::join!(export_future, import_future);
			progress_event_sender
				.send(Ok(tg::progress::Event::Output(())))
				.await
				.ok();
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let progress_event_stream = ReceiverStream::new(progress_event_receiver)
			.attach(abort_handle)
			.attach(indicator_total_task_abort_handle);
		Ok(progress_event_stream)
	}

	async fn set_push_progress_indicator_totals(
		self,
		arg: &tg::push::Arg,
		progress: &crate::progress::Handle<()>,
	) {
		let mut item_data_futures = arg
			.items
			.iter()
			.map(|item| {
				let server = self.clone();
				async move {
					loop {
						match item {
							tangram_either::Either::Left(ref process) => {
								let item_data = server
									.get_push_item_data(Either::Left(process))
									.await
									.map_err(|source| {
										tg::error!(!source, "failed to get item data")
									})?;
								let mut complete = item_data.count.is_some();
								if arg.commands {
									complete = complete
										&& item_data.commands_count.is_some()
										&& item_data.commands_weight.is_some();
								}
								if arg.logs {
									complete = complete
										&& item_data.logs_count.is_some()
										&& item_data.logs_weight.is_some();
								}
								if arg.outputs {
									complete = complete
										&& item_data.outputs_count.is_some()
										&& item_data.outputs_weight.is_some();
								}
								if complete {
									break Ok::<_, tg::Error>(Either::Left(item_data));
								}
							},
							tangram_either::Either::Right(ref object) => {
								let item_data = server
									.get_push_item_data(Either::Right(object))
									.await
									.map_err(|source| {
										tg::error!(!source, "failed to get item data")
									})?;
								if item_data.count.is_some() && item_data.weight.is_some() {
									break Ok::<_, tg::Error>(Either::Right(item_data));
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
		while let Some(Ok(metadata)) = item_data_futures.next().await {
			match metadata {
				Either::Left(metadata) => {
					if let Some(count) = metadata.count {
						total_processes += count;
						progress.set_total("processes", total_processes);
					}
					if arg.commands {
						if let Some(commands_count) = metadata.commands_count {
							total_objects += commands_count;
						}
						if let Some(commands_weight) = metadata.commands_weight {
							total_bytes += commands_weight;
						}
					}
					if arg.logs {
						if let Some(logs_count) = metadata.logs_count {
							total_objects += logs_count;
						}
						if let Some(logs_weight) = metadata.logs_weight {
							total_bytes += logs_weight;
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
					progress.set_total("objects", total_objects);
					progress.set_total("bytes", total_bytes);
				},
				Either::Right(metadata) => {
					if let Some(count) = metadata.count {
						total_objects += count;
						progress.set_total("objects", total_objects);
					}
					if let Some(weight) = metadata.weight {
						total_bytes += weight;
						progress.set_total("bytes", total_bytes);
					}
				},
			}
		}
	}

	async fn get_push_item_data(
		&self,
		id: Either<&tg::process::Id, &tg::object::Id>,
	) -> tg::Result<ItemData> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object metadata.
		let p = connection.p();
		let statement = match id {
			Either::Left(_) => {
				formatdoc!(
					"
						select id, count, complete, commands_count, commands_complete, commands_weight, logs_count, logs_complete, logs_weight, outputs_count, objects_complete, outputs_weight
						from processes
						where id = {p}1;
					",
				)
			},
			Either::Right(_) => {
				formatdoc!(
					"
						select id, count, complete, weight
						from objects
						where id = {p}1;
					"
				)
			},
		};
		let params = db::params![id];
		let Some(output) = connection
			.query_optional_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Err(tg::error!("could not find process"));
		};

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}
}

/// Combined object to aggregate totals.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct ItemData {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	commands_count: Option<u64>,
	#[serde(default, skip_serializing_if = "is_false")]
	commands_complete: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	commands_weight: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	count: Option<u64>,
	#[serde(default, skip_serializing_if = "is_false")]
	complete: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	weight: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	logs_count: Option<u64>,
	#[serde(default, skip_serializing_if = "is_false")]
	logs_complete: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	logs_weight: Option<u64>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	outputs_count: Option<u64>,
	#[serde(default, skip_serializing_if = "is_false")]
	outputs_complete: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	outputs_weight: Option<u64>,
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
