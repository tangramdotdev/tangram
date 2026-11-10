use {
	crate::Server,
	futures::{FutureExt as _, StreamExt as _},
	tangram_client::prelude::*,
	tangram_messenger::{Messenger as _, Payload},
};

pub use tangram_server_control::*;

impl Server {
	#[must_use]
	pub fn control_read_deadline(&self) -> tokio::time::Instant {
		tokio::time::Instant::now() + self.config.control.read_timeout
	}

	pub async fn read_control_response_until<T>(
		&self,
		deadline: tokio::time::Instant,
		future: impl Future<Output = tg::Result<T>>,
	) -> tg::Result<T> {
		tokio::time::timeout_at(deadline, future)
			.await
			.map_err(|error| {
				tg::error!(!error, "timed out waiting for the control read response")
			})?
	}

	pub async fn request_control<I, O, Response>(
		&self,
		arg: SendControlRequestArg<
			I,
			O,
			Response,
			impl Fn(String) -> O + Clone + Send + Sync + 'static,
			impl Fn(&I) -> bool + Clone + Send + Sync + 'static,
			impl Fn(I) -> tg::Result<Option<(String, Response)>> + Clone + Send + Sync + 'static,
		>,
	) -> tg::Result<Response>
	where
		I: Payload,
		Response: Send + 'static,
		O: Clone + Payload,
	{
		self.send_control_request(arg).await?.await
	}

	pub async fn send_control_request<I, O, Response>(
		&self,
		arg: SendControlRequestArg<
			I,
			O,
			Response,
			impl Fn(String) -> O + Clone + Send + Sync + 'static,
			impl Fn(&I) -> bool + Clone + Send + Sync + 'static,
			impl Fn(I) -> tg::Result<Option<(String, Response)>> + Clone + Send + Sync + 'static,
		>,
	) -> tg::Result<futures::future::BoxFuture<'static, tg::Result<Response>>>
	where
		I: Payload,
		Response: Send + 'static,
		O: Clone + Payload,
	{
		let SendControlRequestArg {
			ack,
			client_subject,
			is_ack,
			is_not_holder,
			marker: _,
			options,
			request,
			resend,
			response,
			server_subject,
		} = arg;
		let server = self.clone();
		let Options { retry, timeout } = options;

		let responses = server
			.messenger
			.subscribe::<I>(client_subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the response"))?;
		server
			.messenger
			.publish(server_subject.clone(), request.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the request"))?;
		let future = async move {
			let mut resend = resend;
			let mut responses = std::pin::pin!(responses);

			let mut acknowledged = false;
			let mut initial = true;
			let mut not_holder = None;
			let mut not_holder_delay = retry.backoff;
			let mut not_holder_waited = false;
			let max_delay = retry.max_delay;
			let mut retries = std::pin::pin!(tangram_futures::retry::stream(retry));
			loop {
				// After a response from a server that does not hold the item, wait for a holder's response before sending the request again, without spending a retry, because the response shows that a handoff is in progress.
				let mut timeout = timeout;
				if let Some(delay) = not_holder.take() {
					timeout = delay;
					not_holder_waited = true;
				} else if not_holder_waited && !acknowledged {
					not_holder_waited = false;
					server
						.messenger
						.publish(server_subject.clone(), request.clone())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the request"))?;
				} else if !acknowledged && !initial {
					if resend.is_none() && retries.next().await.is_none() {
						return Err(tg::error!(
							"timed out waiting for the request acknowledgement"
						));
					}

					server
						.messenger
						.publish(server_subject.clone(), request.clone())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the request"))?;
				}

				initial = false;
				let receive = async {
					loop {
						let message = responses
							.next()
							.await
							.ok_or_else(|| tg::error!("the response stream ended"))?
							.map_err(|source| {
								tg::error!(!source, "failed to receive the response")
							})?;
						if is_ack(&message.payload) {
							return Ok::<_, tg::Error>(ReceiveControlRequestOutput::Ack);
						}
						if let Some(id) = is_not_holder(&message.payload) {
							return Ok(ReceiveControlRequestOutput::NotHolder { id });
						}
						let Some((id, response)) = response(message.payload)? else {
							continue;
						};
						return Ok(ReceiveControlRequestOutput::Response { id, response });
					}
				};

				// Send the request again when the caller asks, unless a holder has acknowledged it, because the item may have a new holder.
				let result = tokio::select! {
					result = tokio::time::timeout(timeout, receive) => result,
					item = async { resend.as_mut().unwrap().next().await }, if resend.is_some() && !acknowledged => {
						if item.is_none() {
							resend = None;
						}
						continue;
					},
				};

				match result {
					Ok(Ok(ReceiveControlRequestOutput::Ack)) => acknowledged = true,
					Ok(Ok(ReceiveControlRequestOutput::NotHolder { id })) => {
						let ack = ack(id);
						server
							.messenger
							.publish(server_subject.clone(), ack)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to acknowledge the control response")
							})?;
						acknowledged = false;
						not_holder = Some(not_holder_delay);
						not_holder_delay = (not_holder_delay * 2).min(max_delay);
					},
					Ok(Ok(ReceiveControlRequestOutput::Response { id, response })) => {
						let ack = ack(id);
						server
							.messenger
							.publish(server_subject.clone(), ack)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to acknowledge the control response")
							})?;

						return Ok(response);
					},
					Ok(Err(error)) => return Err(error),
					Err(_) => {
						crate::checkpoint!(
							server,
							"control.request.timeout",
							subject = server_subject.clone(),
						)
						.await;
					},
				}
			}
		};
		Ok(future.boxed())
	}
}
