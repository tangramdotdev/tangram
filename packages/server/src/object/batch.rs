use {
	crate::Session,
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_object_store::prelude::*,
};

impl Session {
	pub async fn post_object_batch(&self, arg: tg::object::batch::Arg) -> tg::Result<()> {
		if arg.objects.is_empty() {
			return Ok(());
		}

		let location = self.server.location(arg.location.as_ref())?;

		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.post_object_batch_local(arg).await?;
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.post_object_batch_region(arg, region).await?;
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.post_object_batch_remote(arg, remote, region).await?;
			},
		}

		Ok(())
	}

	async fn post_object_batch_local(&self, arg: tg::object::batch::Arg) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let grant_expires_at = now
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();

		// Store the objects.
		let principal = self.context.principal.clone();
		let put_args: Vec<_> = arg
			.objects
			.iter()
			.map(|object| crate::object::store::PutArg {
				bytes: Some(object.bytes.clone()),
				cache_pointer: None,
				id: object.id.clone(),
				principal: principal.clone(),
				stored_at: now,
			})
			.collect();
		self.server
			.object_store
			.put_batch(put_args)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the objects"))?;

		// Create the index args.
		let mut put_object_args = Vec::with_capacity(arg.objects.len());
		let mut put_grant_args = Vec::with_capacity(arg.objects.len());
		for object in &arg.objects {
			// Deserialize the object.
			let data = tg::object::Data::deserialize(object.id.kind(), object.bytes.clone())
				.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;

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

			// Create the arg.
			let arg = tangram_index::object::put::Arg {
				cache_entry: None,
				children,
				id: object.id.clone(),
				metadata,
				stored: tangram_index::object::Stored::default(),
				touched_at: now,
			};

			put_object_args.push(arg);

			if let Some(principal) = &principal {
				put_grant_args.push(tangram_index::grant::put::Arg {
					created_at: now,
					creator: Some(principal.clone()),
					expires_at: Some(grant_expires_at),
					permission: tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Node,
					),
					principal: principal.clone().into(),
					resource: object.id.clone().into(),
				});
			}
		}

		// Spawn a task to index the objects.
		self.server
			.index_tasks
			.spawn(|_| {
				let session = self.clone();
				async move {
					if let Err(error) = session
						.server
						.index
						.batch(tangram_index::batch::Arg {
							put_grants: put_grant_args,
							put_objects: put_object_args,
							..Default::default()
						})
						.await
					{
						tracing::error!(error = %error.trace(), "failed to put object batch to index");
					}
				}
			})
			.detach();

		Ok(())
	}

	async fn post_object_batch_region(
		&self,
		arg: tg::object::batch::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::object::batch::Arg {
			location: Some(location.into()),
			..arg
		};
		client.post_object_batch(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to post the object batch"),
		)?;
		Ok(())
	}

	async fn post_object_batch_remote(
		&self,
		arg: tg::object::batch::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::object::batch::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		client.post_object_batch(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to post the object batch"),
		)?;
		Ok(())
	}

	pub(crate) async fn post_object_batch_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Read the body.
		let bytes = request
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the request body"))?;

		// Deserialize the arg.
		let arg = tg::object::batch::Arg::deserialize(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the request"))?;

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
		self.post_object_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to post the object batch"))?;

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

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
