use {
	crate::prelude::*,
	futures::{
		StreamExt as _, TryStreamExt as _,
		stream::{self, BoxStream},
	},
	std::sync::{Arc, Mutex},
	tangram_futures::task::Task,
	tangram_http::flow::{Consumption, Receiver, Sender},
	tokio::sync::{mpsc, watch},
};

#[cfg(test)]
mod tests;

pub struct Connection {
	input: Input,
	notifications: Option<BoxStream<'static, tg::Result<tg::sync::Message>>>,
	updates: Updates,
}

pub struct Input {
	config: tg::sync::Config,
	receiver: Arc<Mutex<Receiver>>,
	sender: mpsc::Sender<tg::Result<(tg::sync::Message, usize)>>,
}

#[derive(Clone)]
pub struct Updates {
	sender: watch::Sender<Update>,
}

#[derive(Clone, Copy, Default)]
struct Update {
	config: Option<tg::sync::Config>,
	consumption: Consumption,
}

struct State {
	input: BoxStream<'static, tg::Result<tg::sync::Message>>,
	pending: Option<tg::sync::Message>,
	updates: watch::Receiver<Update>,
	window: Option<(tg::sync::Config, Sender)>,
}

impl Connection {
	pub fn new(
		config: tg::sync::Config,
	) -> tg::Result<(Self, BoxStream<'static, tg::Result<tg::sync::Message>>)> {
		let (input, messages, consumption) = Input::new(config)?;
		let notifications = stream::once(async move { Ok(tg::sync::Message::Config(config)) })
			.chain(consumption.map(|consumption| Ok(tg::sync::Message::Consumption(consumption))))
			.boxed();
		let connection = Self {
			input,
			notifications: Some(notifications),
			updates: Updates::new(),
		};

		Ok((connection, messages))
	}

	pub fn send_sync_messages(
		&mut self,
		output: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> BoxStream<'static, tg::Result<tg::sync::Message>> {
		let notifications = self.notifications.take().unwrap();
		let output = self.updates.send_sync_messages(output);
		let output = stream::select_with_strategy(notifications, output, |(): &mut ()| {
			stream::PollNext::Left
		})
		.boxed();
		stream::unfold((output, false), |(mut output, ended)| async move {
			if ended {
				return None;
			}
			let message = output.next().await?;
			let ended = matches!(&message, Err(_) | Ok(tg::sync::Message::End));
			Some((message, (output, ended)))
		})
		.boxed()
	}

	#[must_use]
	pub fn receive_sync_messages(
		self,
		mut input: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> Task<()> {
		Task::spawn(move |_| async move {
			let outcome = async {
				while let Some(message) = input.try_next().await? {
					match message {
						tg::sync::Message::Config(config) => {
							self.updates.set_sync_config(config)?;
						},
						tg::sync::Message::Consumption(consumption) => {
							self.updates.update_sync_consumption(consumption)?;
						},
						message => {
							let end = matches!(message, tg::sync::Message::End);
							self.input.receive_sync_message(message)?;
							if end {
								return Ok(());
							}
						},
					}
				}
				Err(tg::error!("the sync stream closed before completion"))
			}
			.await;
			if let Err(error) = outcome {
				self.input.sender.send(Err(error)).await.ok();
			}
		})
	}
}

impl Input {
	pub fn new(
		config: tg::sync::Config,
	) -> tg::Result<(
		Self,
		BoxStream<'static, tg::Result<tg::sync::Message>>,
		BoxStream<'static, Consumption>,
	)> {
		config.validate()?;
		let (sender, messages) = mpsc::channel(config.limits.messages.try_into().unwrap());
		let receiver = Arc::new(Mutex::new(Receiver::new(config.limits)));
		let (consumption_sender, consumption_receiver) = watch::channel(Consumption::default());
		let input = Self {
			config,
			receiver: receiver.clone(),
			sender,
		};
		let output = stream::try_unfold(
			(receiver, consumption_sender, messages, None),
			move |(receiver, sender, mut messages, pending)| async move {
				if let Some(size) = pending {
					let consumption = receiver
						.lock()
						.unwrap()
						.consume(size)
						.map_err(|source| tg::error!(!source, "invalid sync consumption"))?;
					if let Some(consumption) = consumption {
						sender.send_replace(consumption);
					}
				}
				let message = messages.recv().await;
				let Some(message) = message else {
					if let Some(consumption) = receiver.lock().unwrap().flush() {
						sender.send_replace(consumption);
					}
					return Ok(None);
				};
				let (message, size) = message?;
				Ok(Some((message, (receiver, sender, messages, Some(size)))))
			},
		)
		.boxed();
		let consumption = stream::unfold(consumption_receiver, |mut receiver| async move {
			if receiver.changed().await.is_err() {
				return None;
			}
			let consumption = *receiver.borrow_and_update();
			Some((consumption, receiver))
		})
		.boxed();
		Ok((input, output, consumption))
	}

	pub fn receive_sync_message(&self, message: tg::sync::Message) -> tg::Result<()> {
		let size = self.config.sync_message_size(&message)?;
		self.receiver
			.lock()
			.unwrap()
			.receive(size)
			.map_err(|source| tg::error!(!source, "the sync window was exceeded"))?;
		self.sender
			.try_send(Ok((message, size)))
			.map_err(|source| {
				tg::error!(!source, "the sync input closed or exceeded its window")
			})?;
		Ok(())
	}
}

impl Updates {
	#[must_use]
	pub fn new() -> Self {
		let (sender, _) = watch::channel(Update::default());
		Self { sender }
	}

	pub fn set_sync_config(&self, config: tg::sync::Config) -> tg::Result<()> {
		config.validate()?;
		if self.sender.borrow().config.is_some() {
			return Err(tg::error!("received a duplicate sync configuration"));
		}
		self.sender
			.send_modify(|update| update.config = Some(config));
		Ok(())
	}

	pub fn update_sync_consumption(&self, consumption: Consumption) -> tg::Result<()> {
		let previous = self.sender.borrow().consumption;
		if consumption.bytes < previous.bytes || consumption.messages < previous.messages {
			return Err(tg::error!("received decreasing sync consumption"));
		}
		self.sender
			.send_modify(|update| update.consumption = consumption);
		Ok(())
	}

	pub fn send_sync_messages(
		&self,
		input: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> BoxStream<'static, tg::Result<tg::sync::Message>> {
		let state = State {
			input,
			pending: None,
			updates: self.sender.subscribe(),
			window: None,
		};
		stream::try_unfold(state, |mut state| async move {
			loop {
				let update = *state.updates.borrow_and_update();
				if state.window.is_none()
					&& let Some(config) = update.config
				{
					state.window = Some((config, Sender::new(config.limits)));
				}
				if let Some((config, window)) = &mut state.window {
					window
						.update(update.consumption)
						.map_err(|source| tg::error!(!source, "invalid sync consumption"))?;
					if let Some(message) = state.pending.take() {
						let size = config.sync_message_size(&message)?;
						if window.available(size) {
							window.send(size).map_err(|source| {
								tg::error!(!source, "the sync window was exceeded")
							})?;
							return Ok(Some((message, state)));
						}
						state.pending = Some(message);
					}
				}
				tokio::select! {
					biased;
					update = state.updates.changed() => {
						update.map_err(|source| tg::error!(!source, "the sync consumption channel closed"))?;
					},
					message = state.input.try_next(), if state.window.is_some() && state.pending.is_none() => {
						let Some(message) = message? else { return Ok(None); };
						state.pending = Some(message);
					},
				}
			}
		})
		.boxed()
	}
}

impl Default for Updates {
	fn default() -> Self {
		Self::new()
	}
}
