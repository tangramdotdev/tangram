use {
	crate::{Session, sync::get::State},
	futures::{TryStreamExt as _, future},
	std::{collections::HashMap, pin::pin, sync::Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::Messenger as _,
};

pub struct Notification {
	subject: String,
	waiters: Mutex<HashMap<tg::Id, Vec<tg::sync::notification::Request>>>,
}

#[derive(Clone)]
pub(crate) struct Request(pub tg::sync::notification::Request);

pub(crate) struct Response(pub tg::sync::notification::Response);

impl Notification {
	pub fn new(token: &tg::sync::Token) -> Self {
		let subject = subject(token);
		let waiters = Mutex::new(HashMap::new());
		Self { subject, waiters }
	}
}

impl Session {
	pub(super) async fn sync_get_notification_task(
		&self,
		state: &State,
		stopper: Stopper,
	) -> tg::Result<()> {
		let Some(notification) = &state.notification else {
			return Ok(());
		};
		let subject = format!("{}.server", notification.subject);
		let requests = self
			.server
			.messenger
			.subscribe::<Request>(subject)
			.await
			.map_err(|error| tg::error!(!error, "failed to subscribe to the sync requests"))?;
		let mut requests = pin!(requests);
		let mut stop = pin!(stopper.wait());
		loop {
			let next = pin!(requests.try_next());
			let message = match future::select(next, stop.as_mut()).await {
				future::Either::Left((message, _)) => message,
				future::Either::Right(((), _)) => break,
			};
			let Some(message) =
				message.map_err(|error| tg::error!(!error, "failed to receive a sync request"))?
			else {
				break;
			};
			let request = message.payload.0;

			// Refuse a process request for aspects this sync does not carry.
			if let tg::sync::notification::Request::Process(process) = &request {
				let carried = (!process.children || state.arg.process_children)
					&& (!process.commands || state.arg.process_commands)
					&& (!process.errors || state.arg.process_errors)
					&& (!process.logs || state.arg.process_logs)
					&& (!process.outputs || state.arg.process_outputs);
				if !carried {
					self.sync_get_notification_respond(notification, &request, false)
						.await?;
					continue;
				}
			}

			// Hold the waiters while checking the graph so that a concurrent store cannot miss the request.
			let stored = {
				let mut waiters = notification.waiters.lock().unwrap();
				let stored = state.graph.lock().unwrap().is_node_stored(&request.node());
				if !stored {
					waiters
						.entry(request.node())
						.or_default()
						.push(request.clone());
				}
				stored
			};
			if stored {
				self.sync_get_notification_respond(notification, &request, true)
					.await?;
			}
		}

		Ok(())
	}

	pub(super) async fn sync_get_notification_notify(
		&self,
		state: &State,
		ids: impl IntoIterator<Item = tg::Id>,
	) -> tg::Result<()> {
		let Some(notification) = &state.notification else {
			return Ok(());
		};
		let requests = {
			let mut waiters = notification.waiters.lock().unwrap();
			ids.into_iter()
				.filter_map(|id| waiters.remove(&id))
				.flatten()
				.collect::<Vec<_>>()
		};
		for request in requests {
			self.sync_get_notification_respond(notification, &request, true)
				.await?;
		}

		Ok(())
	}

	pub(super) async fn sync_get_notification_finish(&self, state: &State) -> tg::Result<()> {
		let Some(notification) = &state.notification else {
			return Ok(());
		};
		let requests = std::mem::take(&mut *notification.waiters.lock().unwrap())
			.into_values()
			.flatten()
			.collect::<Vec<_>>();
		for request in requests {
			let stored = state.graph.lock().unwrap().is_node_stored(&request.node());
			self.sync_get_notification_respond(notification, &request, stored)
				.await?;
		}

		Ok(())
	}

	async fn sync_get_notification_respond(
		&self,
		notification: &Notification,
		request: &tg::sync::notification::Request,
		available: bool,
	) -> tg::Result<()> {
		let subject = format!("{}.client.{}", notification.subject, request.id());
		let payload = Response(request.response(available));
		self.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|error| tg::error!(!error, "failed to publish the sync response"))?;

		Ok(())
	}
}

impl tangram_messenger::Payload for Request {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let request = tangram_serialize::from_slice(&bytes)
			.map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(request))
	}
}

impl tangram_messenger::Payload for Response {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let response = tangram_serialize::from_slice(&bytes)
			.map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(response))
	}
}

pub(crate) fn subject(token: &tg::sync::Token) -> String {
	format!("syncs.{}.notification", token.body.id)
}
