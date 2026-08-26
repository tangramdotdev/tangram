use {
	crate::Session,
	num::ToPrimitive as _,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_store::prelude::*,
};

impl Session {
	pub async fn post_object_batch(
		&self,
		arg: tg::object::batch::Arg,
	) -> tg::Result<tg::object::batch::Output> {
		if arg.objects.is_empty() {
			return Ok(tg::object::batch::Output::default());
		}

		let location = self.server.location(arg.location.as_ref())?;

		let (mut output, trusted) = match location.clone() {
			tg::Location::Local(tg::location::Local { region: None }) => {
				(self.post_object_batch_local(arg).await?, false)
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => (self.post_object_batch_region(arg, region).await?, false),
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.post_object_batch_remote(arg, remote, region).await?,
		};
		for object in &mut output.objects {
			self.update_tokens_and_location(
				&mut object.options.tokens,
				Some(&mut object.options.location),
				&location,
				trusted,
			)?;
		}

		Ok(output)
	}

	async fn post_object_batch_local(
		&self,
		arg: tg::object::batch::Arg,
	) -> tg::Result<tg::object::batch::Output> {
		let now = self.server.clock.unix_timestamp()?;
		let grant_expires_at = now
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();

		// Get the grant subject.
		let grant_subject = match &self.context.principal {
			tg::Principal::Anonymous => Some(tg::authorization::Subject::Public),
			tg::Principal::Root => None,
			principal => Some(principal.try_to_subject()?),
		};
		// Create the store and index args.
		let mut batch_objects = BTreeSet::new();
		let mut object_children = BTreeMap::new();
		let mut object_children_with_tokens = BTreeMap::new();
		let mut put_args = Vec::with_capacity(arg.objects.len());
		let mut put_object_args = Vec::with_capacity(arg.objects.len());
		for object in &arg.objects {
			batch_objects.insert(object.id.clone());

			// Deserialize the object.
			let data = tg::object::Data::deserialize(object.id.kind(), object.bytes.clone())
				.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;

			// Create the store arg.
			let length = match &data {
				tg::object::Data::Blob(blob) => Some(blob.length()),
				_ => None,
			};
			put_args.push(crate::store::object::put::Arg {
				bytes: Some(object.bytes.clone()),
				checkout_pointer: None,
				id: object.id.clone(),
				length,
				stored_at: now,
			});

			// Get the children.
			let mut children = BTreeSet::new();
			data.children(&mut children);
			object_children.insert(object.id.clone(), children.clone());
			object_children_with_tokens.insert(object.id.clone(), object.children.clone());

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
				checkout: None,
				children,
				id: object.id.clone(),
				metadata,
				storage: tangram_index::object::Storage::default(),
				time_to_touch: self.server.config.object.time_to_touch,
				touched_at: now,
			};

			put_object_args.push(arg);
		}

		// Store the objects.
		self.server
			.store
			.put_object_batch(put_args)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the objects"))?;

		// Determine which objects can receive subtree grants.
		let mut subtree_objects = BTreeSet::new();
		loop {
			let mut changed = false;
			for object in &arg.objects {
				if subtree_objects.contains(&object.id) {
					continue;
				}
				let children = object_children.get(&object.id).unwrap();
				let children_with_tokens = object_children_with_tokens.get(&object.id).unwrap();
				if self
					.post_object_batch_authorize(
						children_with_tokens,
						children,
						&subtree_objects,
						&batch_objects,
					)
					.await?
				{
					subtree_objects.insert(object.id.clone());
					changed = true;
				}
			}
			if !changed {
				break;
			}
		}

		let mut put_grant_args = Vec::with_capacity(arg.objects.len());
		for object in &arg.objects {
			if let Some(grant_subject) = &grant_subject {
				let permission = if subtree_objects.contains(&object.id) {
					tg::authorization::permission::object::Permission::Subtree
				} else {
					tg::authorization::permission::object::Permission::Node
				};
				put_grant_args.push(tangram_index::grant::put::Arg {
					created_at: now,
					creator: Some(self.context.principal.clone()),
					implicit: Some(Some(grant_expires_at)),
					permissions: tg::authorization::Permission::Object(permission).into(),
					subject: grant_subject.clone(),
					resource: object.id.clone().into(),
					time_to_touch: Some(self.server.config.object.grant_time_to_touch),
				});
			}
		}
		let account = self.usage_account(&self.context.principal).await?;
		let mut account_objects = batch_objects.clone();
		for children in object_children.values() {
			for child in children {
				account_objects.remove(child);
			}
		}

		// Index the objects.
		let index_arg = tangram_index::batch::Arg {
			items: put_object_args
				.into_iter()
				.map(tangram_index::batch::Item::PutObject)
				.chain(
					put_grant_args
						.into_iter()
						.map(tangram_index::batch::Item::PutGrant),
				)
				.chain(account.into_iter().flat_map(|account| {
					account_objects.iter().cloned().map(move |object| {
						tangram_index::batch::Item::PutAccountObject(
							tangram_index::usage::storage::put::ObjectArg {
								account: account.clone(),
								object,
								touched_at: now,
							},
						)
					})
				}))
				.collect(),
		};
		self.server
			.index_batch(index_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the object batch"))?;

		let objects = arg
			.objects
			.into_iter()
			.map(|object| {
				let permission = if subtree_objects.contains(&object.id) {
					tg::authorization::permission::object::Permission::Subtree
				} else {
					tg::authorization::permission::object::Permission::Node
				};
				let token = self.create_token(
					object.id.clone().into(),
					vec![tg::authorization::Permission::Object(permission)],
					grant_expires_at,
				)?;
				let object = tg::Referent::with_node_and_token(object.id, token);
				Ok(object)
			})
			.collect::<tg::Result<_>>()?;

		Ok(tg::object::batch::Output { objects })
	}

	pub(crate) async fn post_object_batch_authorize(
		&self,
		children: &[tg::Referent<tg::object::Id>],
		actual_children: &BTreeSet<tg::object::Id>,
		batch_subtrees: &BTreeSet<tg::object::Id>,
		batch_objects: &BTreeSet<tg::object::Id>,
	) -> tg::Result<bool> {
		let mut children_map = std::collections::BTreeMap::new();
		for child in children {
			let id = child.node.clone();
			if !actual_children.contains(&id) {
				continue;
			}
			if children_map.insert(id, child.clone()).is_some() {
				return Ok(false);
			}
		}

		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let mut authorization_args = Vec::new();
		for child in actual_children {
			if batch_objects.contains(child) {
				if !batch_subtrees.contains(child) {
					return Ok(false);
				}
				continue;
			}
			let Some(child) = children_map.get(child) else {
				return Ok(false);
			};
			let Some(token) = child.options.tokens.local() else {
				return Ok(false);
			};
			let resource = tg::Selector::Id(child.node.clone().into());
			if !self.authorize_token(&resource, permission.into(), token) {
				authorization_args.push((child.clone(), permission.into()));
			}
		}

		let outputs = self.authorize_batch(authorization_args).await?;
		let authorized = outputs
			.into_iter()
			.all(|output| output.is_some_and(|permissions| permissions.contains(permission)));

		Ok(authorized)
	}

	async fn post_object_batch_region(
		&self,
		arg: tg::object::batch::Arg,
		region: String,
	) -> tg::Result<tg::object::batch::Output> {
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
		let output = client.post_object_batch(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to post the object batch"),
		)?;
		Ok(output)
	}

	async fn post_object_batch_remote(
		&self,
		arg: tg::object::batch::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<(tg::object::batch::Output, bool)> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let trusted = client.trusted();
		let arg = tg::object::batch::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client.post_object_batch(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to post the object batch"),
		)?;
		Ok((output, trusted))
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

		// Get the content type header.
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the content type header"))?;

		// Read the body.
		let bytes = request
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the request body"))?;

		// Deserialize the arg.
		let arg = match content_type
			.as_ref()
			.map(|content_type| (content_type.type_(), content_type.subtype()))
		{
			Some((mime::APPLICATION, mime::JSON)) => serde_json::from_slice(&bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the request"))?,
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::OCTET_STREAM)) => {
				tg::object::batch::Arg::deserialize(bytes)
					.map_err(|error| tg::error!(!error, "failed to deserialize the request"))?
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid content type"));
			},
		};

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
		let output = self
			.post_object_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to post the object batch"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder()
			.json(output)
			.map_err(|error| tg::error!(!error, "failed to serialize the response"))?
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
