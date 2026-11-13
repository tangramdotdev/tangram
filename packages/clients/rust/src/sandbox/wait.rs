use {
	crate::prelude::*,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::BoxStream,
	},
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
	pub location: Option<tg::location::Arg>,

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
	End,
	Outcome(tg::sandbox::outcome::Data),
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
}

impl tg::Sandbox {
	pub async fn wait(
		&self,
		options: tg::sandbox::wait::Options,
	) -> tg::Result<tg::sandbox::Outcome> {
		let instance = tg::instance()?;
		self.wait_with_instance(instance, options).await
	}

	pub async fn wait_with_instance<I>(
		&self,
		instance: &I,
		options: tg::sandbox::wait::Options,
	) -> tg::Result<tg::sandbox::Outcome>
	where
		I: tg::Instance,
	{
		let arg = tg::sandbox::wait::Arg {
			location: options.location.or_else(|| self.location()),
			tokens: self.tokens(),
		};
		let id = self.id();
		let data = loop {
			let future = instance.wait_sandbox_future(id, arg.clone()).await?;
			if let Some(data) = future.await? {
				break data;
			}
		};
		let outcome = data.try_into()?;
		self.detach();

		Ok(outcome)
	}
}

impl tg::Session {
	pub async fn try_wait_sandbox_future(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::sandbox::outcome::Data>>>>>
	{
		// Send the request.
		let Some(stream) = self.try_wait_sandbox_stream(id, &arg).await? else {
			return Ok(None);
		};

		// Read the events until the end event, and send the request again with backoff when the stream ends before it, which is how a stream reports a lost connection, and which is safe because a wait may be repeated.
		let session = self.clone();
		let id = id.clone();
		let future = async move {
			let mut stream = stream;
			let mut retries = None;
			loop {
				let mut outcome = None;
				let mut ended = false;
				while let Some(event) = stream.try_next().await? {
					match event {
						Event::End => {
							ended = true;
							break;
						},
						Event::Outcome(data) => {
							outcome = Some(data);
						},
					}
				}
				if ended {
					return Ok(outcome);
				}
				let retries = retries.get_or_insert_with(|| {
					tangram_futures::retry::stream(session.arg().stream_retry).boxed()
				});
				if retries.next().await.is_none() {
					return Err(tg::error!(
						"the sandbox wait stream ended before its end event"
					));
				}
				stream = session
					.try_wait_sandbox_stream(&id, &arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the sandbox"))?;
			}
		};

		Ok(Some(future.boxed()))
	}

	async fn try_wait_sandbox_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::wait::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<Event>>>> {
		// Send the request, retrying with backoff while it cannot reach a server or its stream fails before the response, which is safe because a wait may be repeated.
		let path = format!("/sandboxes/{id}/wait");
		let uri = Uri::builder().path(&path).build().unwrap();
		let options = self.arg().stream_retry;
		let response = tangram_futures::retry(&options, || {
			let request = http::request::Builder::default()
				.method(http::Method::POST)
				.uri(uri.clone())
				.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
				.arg(arg, tangram_http::body::Empty::new())
				.map_err(|error| tg::error!(!error, "failed to serialize the arg"));
			async move { self.send_attempt(request?.unwrap()).await }
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to send the request"))?;

		// Read the response.
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
		let stream = crate::http::sse_until_lost(response)
			.and_then(|event| future::ready(event.try_into()))
			.boxed();

		Ok(Some(stream))
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::End => tangram_http::sse::Event {
				event: Some("end".to_owned()),
				..Default::default()
			},
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
			Some("end") => Ok(Self::End),
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
