use {
	crate::{
		Component,
		control::{ClientMessage, ServerMessage, connection},
	},
	dashmap::DashSet,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_messenger::Messenger as _,
	tangram_server_core::Session,
};

/// The record of a process in the index and its data.
pub(crate) type Indexed = (tangram_index::process::Process, Option<tg::process::Data>);

pub(crate) struct Arg<'a> {
	/// The time until which a read waits for the holder's reply, and a write waits for the holder of a process that the index does not have.
	pub deadline: tokio::time::Instant,

	/// Whether to wait for indexing, and then for a holder until the deadline, if the index does not have the process.
	pub exhaustive: bool,

	/// Whether the index answers when the holder does not. A read sets this, so that the index answers for a starting process and for a holder that does not reply by the deadline. A write does not, so that its request is held until the process starts, and only a finished process or a missing one ends its wait.
	pub fallback: bool,

	pub id: &'a tg::process::Id,

	/// The retry and timeout of the request to the holder.
	pub options: tangram_server_core::control::Options,

	pub request: tg::process::control::ServerRequestArg,

	pub source: tg::process::Source,

	/// The status wakeups, each of which reads the process from the index again.
	pub wakeups: Option<&'a mut BoxStream<'static, ()>>,
}

pub(crate) struct Output {
	/// The process in the index, if it was read.
	pub indexed: Option<Indexed>,

	/// The holder's reply, if the holder replied before the index answered.
	pub reply: Option<tg::Result<tg::process::control::ClientResponseOutput>>,
}

impl Component {
	/// Send a request to the holder of a process while reading the process from the index, and return the holder's reply or the index's answer, whichever is decisive first. Return `None` if the wakeups end.
	pub(crate) async fn request_process_holder(
		&self,
		session: &Session,
		arg: Arg<'_>,
		index_required: impl Fn(&tg::process::control::ClientResponseOutput) -> bool + Send,
	) -> tg::Result<Option<Output>> {
		let Arg {
			deadline,
			exhaustive,
			fallback,
			id,
			options,
			request,
			source,
			mut wakeups,
		} = arg;
		let get = matches!(request, tg::process::control::ServerRequestArg::Get(_));

		// Read the process from the index.
		let mut index_future = (!source.is_runner()).then(|| {
			self.request_process_holder_index(session, id, exhaustive, get)
				.boxed()
		});
		let mut indexed: Option<Option<Indexed>> = None;

		// Send a read's request to the holder. A write sends its request once the index is read, so that the request is held while the process is starting.
		let mut request = Some((request, options));
		let mut request_future = if source.is_index() || !fallback {
			None
		} else {
			request.take().map(|(request, options)| {
				self.request_process_holder_inner(session, id, request, options, None)
					.boxed()
			})
		};

		loop {
			// A read waits for the holder until the deadline, and a write does so only if the index does not have the process, because only a holder can then show that the process exists.
			let missing = matches!(
				&indexed,
				Some(indexed) if !Self::process_holder_index_has_status(indexed.as_ref()),
			);
			let wait_until_deadline = (fallback && !source.is_index()) || missing;

			tokio::select! {
				result = async { index_future.as_mut().unwrap().await }, if index_future.is_some() => {
					index_future = None;
					let result = result?;

					// Return the index's answer if it is decisive, or if the index does not have the process and the request is not exhaustive.
					if source.is_index()
						|| Self::process_holder_index_is_decisive(result.as_ref(), fallback)
						|| (!exhaustive && !Self::process_holder_index_has_status(result.as_ref()))
					{
						let output = Output {
							indexed: result,
							reply: None,
						};
						return Ok(Some(output));
					}

					// Send a write's request, holding it while the process is starting.
					if let Some((request, options)) = request.take() {
						let starting = result
							.as_ref()
							.and_then(|(process, _)| process.status)
							.is_some_and(|status| status.is_starting());
						let hold = starting.then(|| {
							self.create_wait_process_start_future(session, id)
								.map(|_| ())
								.boxed()
						});
						let future = self
							.request_process_holder_inner(session, id, request, options, hold)
							.boxed();
						request_future = Some(future);
					}
					indexed = Some(result);
				},

				reply = async { request_future.as_mut().unwrap().await }, if request_future.is_some() => {
					let indexed = match &reply {
						// Read the process from the index if the reply requires it, because the holder does not have everything that the index does.
						Ok(output) if !source.is_runner() && index_required(output) => {
							if get {
								tangram_server_core::checkpoint!(session.server, "process.get.index.required", process = %id)
									.await;
							}
							let indexed = match index_future.take() {
								Some(index_future) => index_future.await?,
								None => indexed.flatten(),
							};
							if Self::process_holder_index_is_complete(indexed.as_ref()) {
								indexed
							} else {
								self.try_get_process_with_data_from_index_with_exhaustive(
									session, id, exhaustive,
								)
								.await?
							}
						},

						Ok(_) => indexed.flatten(),

						// Read the process from the index again if the request failed, because the process may have finished.
						Err(_) if !source.is_runner() => {
							self.try_get_process_with_data_from_index_with_exhaustive(
								session, id, exhaustive,
							)
							.await?
						},

						Err(_) => None,
					};
					let output = Output {
						indexed,
						reply: Some(reply),
					};
					return Ok(Some(output));
				},

				() = tokio::time::sleep_until(deadline), if wait_until_deadline => {
					// Answer a read from the index, and a write with the missing process.
					let indexed = if !fallback {
						indexed.flatten()
					} else if source.is_runner() {
						None
					} else {
						self.try_get_process_with_data_from_index_with_exhaustive(
							session, id, exhaustive,
						)
						.await?
					};
					let output = Output {
						indexed,
						reply: None,
					};
					return Ok(Some(output));
				},

				wakeup = async { wakeups.as_mut().unwrap().next().await }, if wakeups.is_some() => {
					if wakeup.is_none() {
						return Ok(None);
					}

					// Read the process from the index again, because its status changed.
					if !source.is_runner() {
						let future = self
							.request_process_holder_index(session, id, exhaustive, get)
							.boxed();
						index_future = Some(future);
					}
				},
			}
		}
	}

	/// Read a process from the index for a request to its holder.
	async fn request_process_holder_index(
		&self,
		session: &Session,
		id: &tg::process::Id,
		exhaustive: bool,
		get: bool,
	) -> tg::Result<Option<Indexed>> {
		let indexed = self
			.try_get_process_with_data_from_index_with_exhaustive(session, id, exhaustive)
			.await;
		if get {
			tangram_server_core::checkpoint!(session.server, "process.get.index", process = %id)
				.await;
		}
		indexed
	}

	/// Send a request to the holder of a process, sending it again after each reply from a server that does not hold the process.
	async fn request_process_holder_inner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		request: tg::process::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
		mut hold: Option<futures::future::BoxFuture<'static, ()>>,
	) -> tg::Result<tg::process::control::ClientResponseOutput> {
		let get = matches!(request, tg::process::control::ServerRequestArg::Get(_));
		let tangram_server_core::control::Options { retry, timeout } = options;
		let mut retries = tangram_futures::retry::stream(retry.clone()).boxed();
		let output = async {
			loop {
				if retries.next().await.is_none() {
					return Err(tg::error!(%id, "failed to find the holder of the process"));
				}
				let retry = retry.clone();
				let options = tangram_server_core::control::Options { retry, timeout };
				let output = self
					.send_process_control_request(
						session,
						id,
						request.clone(),
						options,
						hold.take(),
					)
					.boxed()
					.await?
					.await??;
				if !matches!(
					output,
					tg::process::control::ClientResponseOutput::NotHolder(_)
				) {
					return Ok(output);
				}
			}
		}
		.await;
		if get {
			tangram_server_core::checkpoint!(session.server, "process.get.control", process = %id)
				.await;
		}

		output
	}

	/// Serve the requests to the holder of a process by passing each one to the process's control connection, which answers it or replies that it does not hold the process.
	pub(crate) async fn serve_process_holder(
		&self,
		session: &Session,
		id: &tg::process::Id,
		sender: connection::Sender,
		forwarded_requests: Arc<DashSet<String>>,
	) -> tg::Result<Task<tg::Result<()>>> {
		let subject = format!("processes.{id}.control.server");
		let mut requests = session
			.server
			.messenger
			.subscribe::<ServerMessage>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the message stream"))?;
		let task = Task::spawn(move |_| async move {
			while let Some(message) = requests
				.try_next()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the message"))?
			{
				let message = message.payload.0;
				let acknowledged_request = match &message {
					tg::process::control::ServerMessage::Ack(ack) => Some(ack.id.clone()),
					_ => None,
				};
				if let tg::process::control::ServerMessage::Request(request) = &message {
					forwarded_requests.insert(request.id.clone());
				}
				let low = matches!(
					&message,
					tg::process::control::ServerMessage::Request(
						tg::process::control::ServerRequest {
							arg: tg::process::control::ServerRequestArg::Open(_),
							..
						}
					)
				);
				if low {
					sender.send_low(message).await?;
				} else {
					sender.send(message).await?;
				}
				if let Some(id) = acknowledged_request {
					forwarded_requests.remove(&id);
				}
			}
			Ok::<_, tg::Error>(())
		});

		Ok(task)
	}

	/// Publish a holder's acknowledgement of a request to the server that sent the request.
	pub(crate) async fn publish_process_holder_ack(
		&self,
		session: &Session,
		id: &tg::process::Id,
		ack: tg::process::control::ClientAck,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.control.client.{}", ack.id);
		let payload = ClientMessage(tg::process::control::ClientMessage::Ack(ack));
		session
			.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to publish the process control acknowledgement"
				)
			})?;

		Ok(())
	}

	/// Publish a holder's response to a request to the server that sent the request.
	pub(crate) async fn publish_process_holder_response(
		&self,
		session: &Session,
		id: &tg::process::Id,
		response: tg::process::control::ClientResponse,
	) -> tg::Result<()> {
		let kind = match response.output.as_ref() {
			Some(tg::process::control::ClientResponseOutput::AcquireLease(_)) => "acquire_lease",
			Some(tg::process::control::ClientResponseOutput::Finish(_)) => "finish",
			Some(tg::process::control::ClientResponseOutput::Get(_)) => "get",
			Some(tg::process::control::ClientResponseOutput::GetChildren(_)) => "get_children",
			Some(tg::process::control::ClientResponseOutput::NotHolder(_)) => "not_holder",
			Some(tg::process::control::ClientResponseOutput::Open(_)) => "open",
			Some(tg::process::control::ClientResponseOutput::ReleaseLease(_)) => "release_lease",
			Some(tg::process::control::ClientResponseOutput::Signal(_)) => "signal",
			Some(tg::process::control::ClientResponseOutput::Tty(_)) => "tty",
			None => "error",
		};
		let request = response.id.clone();
		tangram_server_core::checkpoint!(
			session.server,
			"process.control.response.publish",
			kind = %kind,
			process = %id,
			request = request.clone(),
		)
		.await;
		let subject = format!("processes.{id}.control.client.{}", response.id);
		let payload = ClientMessage(tg::process::control::ClientMessage::Response(response));
		session
			.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to publish the process control response")
			})?;
		tangram_server_core::checkpoint!(
			session.server,
			"process.control.response.published",
			kind = %kind,
			process = %id,
			request,
		)
		.await;

		Ok(())
	}

	/// Whether the index's answer ends a request to the holder: the process finished, it lives in a remote, or it is starting and the index answers for it.
	fn process_holder_index_is_decisive(indexed: Option<&Indexed>, fallback: bool) -> bool {
		indexed.is_some_and(|(process, _)| {
			process
				.status
				.is_some_and(|status| status.is_finished() || (fallback && status.is_starting()))
				|| process
					.location
					.as_ref()
					.is_some_and(tg::Location::is_remote)
		})
	}

	/// Whether the index has everything that a holder's reply does not.
	fn process_holder_index_is_complete(indexed: Option<&Indexed>) -> bool {
		indexed.is_some_and(|(process, _)| {
			process
				.status
				.is_some_and(|status| status.is_finished() || status.is_starting())
		})
	}

	/// Whether the index has the process's status, without which only a holder can show that the process exists.
	fn process_holder_index_has_status(indexed: Option<&Indexed>) -> bool {
		indexed.is_some_and(|(process, _)| process.status.is_some())
	}
}
