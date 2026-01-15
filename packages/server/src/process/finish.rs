use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{
		FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future,
		stream::FuturesUnordered,
	},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{collections::BTreeSet, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _},
	tangram_messenger::{self as messenger, prelude::*},
};

#[derive(Clone, Debug)]
pub struct Messages(pub Vec<Message>);

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Message {
	#[tangram_serialize(id = 0)]
	id: tg::process::Id,
}

impl Server {
	pub(crate) async fn finish_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::process::finish::Arg {
				checksum: arg.checksum,
				error: arg.error,
				exit: arg.exit,
				local: None,
				output: arg.output,
				remotes: None,
			};
			client
				.finish_process(id, arg)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to finish the process"))?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// If the task for the process is not the current task, then abort it.
		if self
			.process_tasks
			.try_get_id(id)
			.is_some_and(|task_id| task_id != tokio::task::id())
		{
			self.process_tasks.abort(id);
		}

		let tg::process::finish::Arg {
			mut error,
			output,
			mut exit,
			..
		} = arg;

		// Get the process.
		let Some(tg::process::get::Output { data, .. }) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
		else {
			return Err(tg::error!("failed to find the process"));
		};

		// Get the process's children.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		#[derive(Clone, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			child: tg::process::Id,
			token: Option<String>,
		}
		let statement = formatdoc!(
			"
				select child, token
				from process_children
				where process = {p}1
				order by position;
			"
		);
		let params = db::params![id.to_string()];
		let children = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		// Cancel the children.
		children
			.clone()
			.into_iter()
			.map(|row| {
				let id = row.child;
				let token = row.token;
				async move {
					if let Some(token) = token {
						let arg = tg::process::cancel::Arg {
							local: Some(true),
							remotes: None,
							token,
						};
						self.cancel_process(&id, arg).await.ok();
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		// Verify the checksum if one was provided.
		if let Some(expected) = &data.expected_checksum
			&& exit == 0
		{
			let actual = arg
				.checksum
				.as_ref()
				.ok_or_else(|| tg::error!(%id, "the actual checksum was not set"))?;
			if expected != actual {
				let data = tg::error::Data {
					code: Some(tg::error::Code::ChecksumMismatch),
					message: Some("checksum mismatch".into()),
					values: [
						("expected".into(), expected.to_string()),
						("actual".into(), actual.to_string()),
					]
					.into(),
					..Default::default()
				};
				error = Some(tg::Either::Left(data));
				exit = 1;
			}
		}

		if !self.config.advanced.internal_error_locations
			&& let Some(tg::Either::Left(error)) = &mut error
		{
			error.remove_internal_locations();
		}

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Update the process.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set
					actual_checksum = {p}1,
					depth = null,
					error = {p}2,
					error_code = {p}3,
					finished_at = {p}4,
					heartbeat_at = null,
					output = {p}5,
					exit = {p}6,
					status = {p}7,
					touched_at = {p}8
				where
					id = {p}9 and
					status != 'finished';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let error_data_or_id = error.as_ref().map(|error| match error {
			tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
			tg::Either::Right(id) => id.to_string(),
		});
		let error_code = error.as_ref().and_then(|error| match error {
			tg::Either::Left(data) => data.code.map(|code| code.to_string()),
			tg::Either::Right(_) => None,
		});
		let params = db::params![
			arg.checksum.as_ref().map(ToString::to_string),
			error_data_or_id,
			error_code,
			now,
			output.clone().map(db::value::Json),
			exit,
			tg::process::Status::Finished.to_string(),
			now,
			id.to_string(),
		];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n != 1 {
			return Err(tg::error!("the process was already finished"));
		}

		// Delete the tokens.
		let statement = formatdoc!(
			"
				delete from process_tokens where process = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update token count to 0.
		let statement = formatdoc!(
			"
				update processes
				set token_count = 0
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Publish the put process index message.
		let command = (
			data.command.clone().into(),
			crate::index::message::ProcessObjectKind::Command,
		);
		let errors = data
			.error
			.as_ref()
			.into_iter()
			.flat_map(|error| match error {
				tg::Either::Left(data) => {
					let mut children = BTreeSet::new();
					data.children(&mut children);
					children
						.into_iter()
						.map(|object| {
							let kind = crate::index::message::ProcessObjectKind::Error;
							(object, kind)
						})
						.collect::<Vec<_>>()
				},
				tg::Either::Right(id) => {
					let id = id.clone().into();
					let kind = crate::index::message::ProcessObjectKind::Error;
					vec![(id, kind)]
				},
			});
		let log = data.log.as_ref().map(|id| {
			let id = id.clone().into();
			let kind = crate::index::message::ProcessObjectKind::Log;
			(id, kind)
		});
		let mut outputs = BTreeSet::new();
		if let Some(output) = &output {
			output.children(&mut outputs);
		}
		let outputs = outputs.into_iter().map(|object| {
			let kind = crate::index::message::ProcessObjectKind::Output;
			(object, kind)
		});
		let objects = std::iter::once(command)
			.chain(errors)
			.chain(log)
			.chain(outputs)
			.collect();
		let children = children.into_iter().map(|row| row.child).collect();
		let message = crate::index::Message::PutProcess(crate::index::message::PutProcess {
			children,
			stored: crate::index::ProcessStored::default(),
			id: id.clone(),
			metadata: tg::process::Metadata::default(),
			objects,
			touched_at: now,
		});
		self.tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					let result = server
						.messenger
						.stream_publish(
							"index".to_owned(),
							crate::index::message::Messages(vec![message]),
						)
						.map_err(|source| tg::error!(!source, "failed to publish the message"))
						.and_then(|future| {
							future.map_err(|source| {
								tg::error!(!source, "failed to publish the message")
							})
						})
						.await;
					if let Err(error) = result {
						tracing::error!(?error, "failed to publish the put process index message");
					}
				}
			})
			.detach();

		// Publish the finish message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				let message = Message { id: id.clone() };
				server
					.messenger
					.stream_publish("finish".to_owned(), message)
					.await
					.inspect_err(|error| tracing::error!(%error, %id, "failed to publish"))
					.ok();
			}
		});

		// Publish the status.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.status"), ())
					.await
					.inspect_err(|error| tracing::error!(%error, %id, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}

	pub(crate) async fn finisher_task(&self, config: &crate::config::Finisher) -> tg::Result<()> {
		// Get the messages stream.
		let stream = self.finisher_create_message_stream(config).await?;
		let mut stream = pin!(stream);

		loop {
			// Handle the result.
			let messages = match stream.try_next().await {
				Ok(Some(messages)) => messages,
				Ok(None) => {
					panic!("the stream ended")
				},
				Err(error) => {
					tracing::error!(?error, "failed to get a batch of messages");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Handle the messages.
			let result = self.finisher_handle_messages(config, messages).await;
			if let Err(error) = result {
				tracing::error!(?error, "failed to handle the messages");
				tokio::time::sleep(Duration::from_secs(1)).await;
			} else {
				// Publish finisher progress.
				self.messenger
					.publish("finisher_progress".to_owned(), ())
					.await
					.ok();
			}
		}
	}

	async fn finisher_create_message_stream(
		&self,
		config: &crate::config::Finisher,
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Vec<Message>, messenger::Acker)>>>> {
		let stream = self
			.messenger
			.get_stream("finish".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the finish stream"))?;
		let consumer = stream
			.get_consumer("finish".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the finish consumer"))?;
		let batch_config = messenger::BatchConfig {
			max_bytes: None,
			max_messages: Some(config.message_batch_size.to_u64().unwrap()),
			timeout: Some(config.message_batch_timeout),
		};
		let stream = consumer
			.batch_subscribe::<Messages>(batch_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the stream"))?
			.boxed()
			.map_err(|source| tg::error!(!source, "failed to get a message from the stream"))
			.map_ok(|message| {
				let (messages, acker) = message.split();
				(messages.0, acker)
			})
			.inspect_err(|error| {
				tracing::error!(?error);
			})
			.filter_map(|result| future::ready(result.ok()));
		let stream = tokio_stream::StreamExt::chunks_timeout(
			stream,
			config.message_batch_size,
			config.message_batch_timeout,
		)
		.map(Ok);
		Ok(stream)
	}

	async fn finisher_handle_messages(
		&self,
		_config: &crate::config::Finisher,
		messages: Vec<(Vec<Message>, messenger::Acker)>,
	) -> tg::Result<()> {
		for (messages, acker) in messages {
			for message in messages {
				let process = message.id;
				self.compact_process_log(&process)
					.boxed()
					.await
					.inspect_err(|error| tracing::error!(?error, %process, "failed to compact log"))
					.ok();
			}
			acker
				.ack()
				.await
				.map_err(|source| tg::error!(!source, "failed to acknowledge the message"))?;
		}
		Ok(())
	}

	pub(crate) async fn handle_finish_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Finish the process.
		self.finish_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to finish the process"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().body(Body::empty()).unwrap();
		Ok(response)
	}
}

impl Message {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("missing format byte"));
		}
		let format = bytes[0];
		match format {
			0 => tangram_serialize::from_slice(&bytes[1..])
				.map_err(|source| tg::error!(!source, "failed to deserialize the message")),
			b'{' => serde_json::from_slice(bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the message")),
			_ => Err(tg::error!("invalid format")),
		}
	}
}

impl messenger::Payload for Message {
	fn serialize(&self) -> Result<Bytes, messenger::Error> {
		Message::serialize(self).map_err(messenger::Error::other)
	}

	fn deserialize(bytes: Bytes) -> Result<Self, messenger::Error> {
		Message::deserialize(bytes).map_err(messenger::Error::other)
	}
}

impl messenger::Payload for Messages {
	fn serialize(&self) -> Result<Bytes, messenger::Error> {
		let mut bytes = Vec::new();
		for message in &self.0 {
			let serialized = Message::serialize(message).map_err(messenger::Error::other)?;
			bytes.extend_from_slice(&serialized);
		}
		Ok(bytes.into())
	}

	fn deserialize(bytes: Bytes) -> Result<Self, messenger::Error> {
		let len = bytes.len();
		let mut position = 0usize;
		let mut messages = Vec::new();
		while position < len {
			let message =
				Message::deserialize(&bytes[position..]).map_err(messenger::Error::other)?;
			let serialized = Message::serialize(&message).map_err(messenger::Error::other)?;
			position += serialized.len();
			messages.push(message);
		}
		Ok(Messages(messages))
	}
}
