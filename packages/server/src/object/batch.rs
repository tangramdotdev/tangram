use {
	crate::{Context, Server},
	futures::TryFutureExt as _,
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
	tangram_messenger::prelude::*,
	tangram_store::prelude::*,
};

impl Server {
	pub async fn post_object_batch_with_context(
		&self,
		_context: &Context,
		arg: tg::object::batch::Arg,
	) -> tg::Result<()> {
		if arg.objects.is_empty() {
			return Ok(());
		}

		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		// Store the objects.
		let put_args: Vec<_> = arg
			.objects
			.iter()
			.map(|object| crate::store::PutObjectArg {
				id: object.id.clone(),
				bytes: Some(object.bytes.clone()),
				touched_at: now,
				cache_pointer: None,
			})
			.collect();
		self.store
			.put_object_batch(put_args)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the objects"))?;

		// Create the index messages.
		let mut messages = Vec::with_capacity(arg.objects.len());
		for object in &arg.objects {
			// Deserialize the object.
			let data = tg::object::Data::deserialize(object.id.kind(), object.bytes.clone())
				.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;

			// Get the children.
			let mut children = BTreeSet::new();
			data.children(&mut children);

			// Create the metadata.
			let metadata = tg::object::Metadata {
				node: tg::object::metadata::Node::with_data_and_size(
					&data,
					object.bytes.len().to_u64().unwrap(),
				),
				..Default::default()
			};

			// Create the message.
			let message = crate::index::Message::PutObject(crate::index::message::PutObject {
				cache_entry: None,
				children,
				id: object.id.clone(),
				metadata,
				stored: crate::index::ObjectStored::default(),
				touched_at: now,
			});

			messages.push(message);
		}

		// Spawn a task to publish the index messages.
		self.tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					let payload = crate::index::message::Messages(messages);
					let result = server
						.messenger
						.stream_publish("index".to_owned(), payload)
						.map_err(|source| tg::error!(!source, "failed to publish the messages"))
						.and_then(|future| {
							future.map_err(|source| {
								tg::error!(!source, "failed to publish the messages")
							})
						})
						.await;
					if let Err(error) = result {
						tracing::error!(
							?error,
							"failed to publish the post object batch index messages"
						);
					}
				}
			})
			.detach();

		Ok(())
	}

	pub(crate) async fn handle_post_object_batch_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Read the body.
		let bytes = request
			.bytes()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the request body"))?;

		// Deserialize the arg.
		let arg = tg::object::batch::Arg::deserialize(bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the request"))?;

		// Validate all object IDs match their bytes.
		for object in &arg.objects {
			let actual = tg::object::Id::new(object.id.kind(), &object.bytes);
			if object.id != actual {
				return Err(tg::error!(
					expected = %object.id,
					actual = %actual,
					"invalid object id"
				));
			}
		}

		// Post the object batch.
		self.post_object_batch_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post the object batch"))?;

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
