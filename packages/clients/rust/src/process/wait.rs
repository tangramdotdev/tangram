use {
	crate::prelude::*,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
	},
	tangram_futures::stream::TryExt as _,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub lease: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "tg::process::Source::is_auto")]
	#[tangram_serialize(default, id = 3, skip_serializing_if = "tg::process::Source::is_auto")]
	pub source: tg::process::Source,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	#[tangram_serialize(
		default,
		id = 2,
		skip_serializing_if = "tg::authorization::Tokens::is_empty"
	)]
	pub tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug)]
pub enum Event {
	Outcome(tg::process::outcome::Data),
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
	pub source: tg::process::Source,
}

impl<O> tg::Process<O> {
	pub async fn wait(
		&self,
		options: tg::process::wait::Options,
	) -> tg::Result<tg::process::Outcome> {
		let instance = tg::instance()?;
		self.wait_with_instance(instance, options).await
	}

	pub async fn wait_with_instance<I>(
		&self,
		instance: &I,
		options: tg::process::wait::Options,
	) -> tg::Result<tg::process::Outcome>
	where
		I: tg::Instance,
	{
		let instance = self.instance_with_instance(instance);
		let instance = &instance;
		if options.source.is_auto()
			&& let Some(task) = &self.0.task
		{
			self.wait_stdio().await?;
			let data = task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the task panicked"))??;
			let outcome: tg::process::Outcome = data.try_into()?;
			let location = self.location().and_then(|location| location.to_location());
			outcome.inherit_location(location.as_ref());
			let tokens = self.tokens();
			outcome.inherit_tokens(&tokens);
			self.disarm();
			return Ok(outcome);
		}
		let outcome = options
			.source
			.is_auto()
			.then(|| self.0.outcome.lock().unwrap().take())
			.flatten();
		if let Some(outcome) = outcome {
			self.wait_stdio().await?;
			let location = self.location().and_then(|location| location.to_location());
			outcome.inherit_location(location.as_ref());
			let tokens = self.tokens();
			outcome.inherit_tokens(&tokens);
			self.disarm();
			return Ok(outcome);
		}
		let Some(id) = self.id().right() else {
			return Err(tg::error!(
				"waiting for an unsandboxed process is not supported"
			));
		};
		let location = options.location.or_else(|| self.location());
		let arg = tg::process::wait::Arg {
			lease: self.lease().cloned(),
			location: location.clone(),
			source: options.source,
			tokens: self.tokens(),
		};
		let mut future = instance.wait_process_future(id, arg.clone()).await?;
		self.wait_stdio().await?;
		let data = loop {
			if let Some(data) = future.await? {
				break data;
			}
			future = instance.wait_process_future(id, arg.clone()).await?;
		};
		let outcome: tg::process::Outcome = data.try_into()?;
		let location = location.and_then(|location| location.to_location());
		outcome.inherit_location(location.as_ref());
		let tokens = self.tokens();
		outcome.inherit_tokens(&tokens);
		self.disarm();

		Ok(outcome)
	}

	pub(super) async fn wait_stdio(&self) -> tg::Result<()> {
		let Some(task) = &self.0.stdio_task else {
			return Ok(());
		};
		let result = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the stdio task panicked"))?;
		// Detach deliberately closes the transport used by inherited stdio.
		if !self
			.0
			.connection
			.as_ref()
			.is_some_and(super::connect::Connection::detached)
		{
			result?;
		}
		Ok(())
	}
}

impl tg::Session {
	pub async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>> {
		let method = http::Method::POST;
		let path = format!("/processes/{id}/wait");
		let uri = Uri::builder().path(&path).build().unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.arg(&arg, tangram_http::body::Empty::new())
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let content_type = response
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if !matches!(
			content_type
				.as_ref()
				.map(|content_type| (content_type.type_(), content_type.subtype())),
			Some((mime::TEXT, mime::EVENT_STREAM)),
		) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}
		let stream = response
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();
		let future = stream.boxed().try_last().map_ok(|option| {
			option.map(|event| {
				let Event::Outcome(data) = event;
				data
			})
		});
		Ok(Some(future.boxed()))
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Outcome(outcome) => {
				let data = serde_json::to_string(&outcome)
					.map_err(|error| tg::error!(!error, "failed to serialize the event"))?;
				tangram_http::sse::Event {
					data,
					event: Some("outcome".into()),
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("outcome") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Ok(Self::Outcome(data))
			},
			Some("error") => {
				let error = serde_json::from_str(&value.data)
					.map_err(|error| tg::error!(!error, "failed to deserialize the event"))?;
				Err(error)
			},
			value => Err(tg::error!(?value, "invalid event")),
		}
	}
}
