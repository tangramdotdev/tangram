use {
	crate::Session,
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

pub(crate) struct Authorization {
	pub(super) command_has_subtree_permission: bool,
	pub(super) error_has_subtree_permission: bool,
	pub(super) log_has_subtree_permission: bool,
	pub(super) output_has_subtree_permission: bool,
}

pub(crate) enum ObjectPermissions {
	Authorized(Authorization),
	Capture,
}

pub(crate) struct Options {
	pub defer_index: bool,
	pub location: Option<tg::Location>,
	pub store_data: bool,
	pub sync: Option<tg::Referent<tg::sync::Id>>,
}

impl Session {
	pub(crate) async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<tg::process::put::Output> {
		let location = self.server.location(arg.location.as_ref())?;

		let (mut output, trusted) = match location.clone() {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != self.server.config.region.as_deref() => {
				(self.put_process_region(id, arg, region).await?, false)
			},
			tg::Location::Local(_) => {
				let options = Options {
					defer_index: false,
					location: None,
					store_data: true,
					sync: None,
				};
				(self.put_process_local(id, arg, options).await?, false)
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.put_process_remote(id, arg, remote, region).await?,
		};
		self.update_tokens_and_location(&mut output.tokens, None, &location, trusted)?;

		Ok(output)
	}

	pub(crate) async fn put_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
		options: Options,
	) -> tg::Result<tg::process::put::Output> {
		Self::validate_process_data(&arg.data)?;
		let authorization = self.authorize_process_data(&arg.data).await?;
		let output = self
			.put_process_local_inner(
				id,
				arg,
				ObjectPermissions::Authorized(authorization),
				options,
			)
			.await?;

		Ok(output)
	}

	pub(crate) async fn put_finished_process_local(
		&self,
		id: &tg::process::Id,
		data: tg::process::Data,
		options: Options,
	) -> tg::Result<()> {
		Self::validate_process_data(&data)?;
		let object_permissions = ObjectPermissions::Capture;

		let entry = tg::process::put::Arg {
			data,
			location: None,
		};
		self.put_process_local_inner(id, entry, object_permissions, options)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to store the finished process"))?;

		Ok(())
	}

	fn finished_process_objects(data: &tg::process::Data) -> Vec<tg::Referent<tg::object::Id>> {
		let mut roots = data.command.objects();
		if let Some(error) = &data.error {
			match error {
				tg::Either::Left(data) => {
					let mut children = BTreeSet::new();
					data.children(&mut children);
					roots.extend(children.into_iter().map(tg::Referent::with_node));
				},
				tg::Either::Right(error) => {
					roots.push(error.clone().map(tg::object::Id::Error));
				},
			}
		}
		if let Some(log) = &data.log {
			roots.push(log.clone().map(tg::object::Id::from));
		}
		if let Some(output) = &data.output {
			output.children_with_tokens(&mut roots);
		}
		roots
	}

	pub(super) async fn authorize_process_data(
		&self,
		data: &tg::process::Data,
	) -> tg::Result<Authorization> {
		let mut objects = data.command.objects();
		let command_object_count = objects.len();
		if let Some(error) = &data.error {
			match error {
				tg::Either::Left(data) => {
					let mut children = BTreeSet::new();
					data.children(&mut children);
					objects.extend(children.into_iter().map(tg::Referent::with_node));
				},
				tg::Either::Right(error) => {
					objects.push(error.clone().map(tg::object::Id::Error));
				},
			}
		}
		let error_object_count = objects.len();
		if let Some(log) = &data.log {
			objects.push(log.clone().map(tg::object::Id::from));
		}
		let log_object_count = objects.len();
		if let Some(output) = &data.output {
			output.children_with_tokens(&mut objects);
		}
		let output_object_count = objects.len();
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let permissions = tg::authorization::permission::Set::from_permission(permission);
		let authorizations = self
			.authorize_batch(objects.into_iter().map(|object| (object, permissions)))
			.await?;
		crate::authorization::check_exhaustion(&authorizations)?;
		let has_subtree_permission = |authorizations: &[crate::authorization::Output]| {
			authorizations
				.iter()
				.all(|authorization| authorization.permissions.contains(permission))
		};
		let command_has_subtree_permission =
			has_subtree_permission(&authorizations[..command_object_count]);
		let error_has_subtree_permission =
			has_subtree_permission(&authorizations[command_object_count..error_object_count]);
		let log_has_subtree_permission =
			has_subtree_permission(&authorizations[error_object_count..log_object_count]);
		let output_has_subtree_permission =
			has_subtree_permission(&authorizations[log_object_count..output_object_count]);
		let authorization = Authorization {
			command_has_subtree_permission,
			error_has_subtree_permission,
			log_has_subtree_permission,
			output_has_subtree_permission,
		};

		Ok(authorization)
	}

	pub(crate) async fn put_process_local_inner(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::put::Arg,
		object_permissions: ObjectPermissions,
		options: Options,
	) -> tg::Result<tg::process::put::Output> {
		let Options {
			defer_index,
			location,
			store_data,
			sync,
		} = options;
		let now = self.server.clock.unix_timestamp()?;
		let token_data = arg.data.clone();

		// Preserve the child authorization tokens separately from the canonical process data.
		let children = arg.data.children.take();
		arg.data = arg.data.without_location_and_tokens();
		if let Some(sync) = &sync {
			Self::inherit_process_authorization_tokens_for_sync(&mut arg.data, sync);
		}

		// Create the index arguments.
		let error_objects = arg.data.error.as_ref().map(|error| match error {
			tg::Either::Left(data) => {
				let mut children = BTreeSet::new();
				data.children(&mut children);
				children.into_iter().collect::<Vec<_>>()
			},
			tg::Either::Right(id) => {
				let id = id.node.clone().into();
				vec![id]
			},
		});
		let mut output_objects = BTreeSet::new();
		if let Some(data) = &arg.data.output {
			data.children(&mut output_objects);
		}
		let output_objects = arg
			.data
			.output
			.as_ref()
			.map(|_| output_objects.into_iter().collect::<Vec<_>>());
		let log_object: Option<Option<tg::object::Id>> =
			Some(arg.data.log.clone().map(|log| log.node.into()));
		let principal = match &object_permissions {
			ObjectPermissions::Authorized(_) => self.context.principal.clone(),
			ObjectPermissions::Capture => tg::Principal::Process(id.clone()),
		};
		let (subtree_objects, mut put_object_permissions) = match object_permissions {
			ObjectPermissions::Authorized(authorization) => {
				let Authorization {
					command_has_subtree_permission,
					error_has_subtree_permission,
					log_has_subtree_permission,
					output_has_subtree_permission,
				} = authorization;
				let mut objects = BTreeSet::new();
				if command_has_subtree_permission {
					objects.extend(
						arg.data
							.command
							.objects()
							.into_iter()
							.map(|object| object.node),
					);
				}
				if error_has_subtree_permission && let Some(error_objects) = &error_objects {
					objects.extend(error_objects.iter().cloned());
				}
				if log_has_subtree_permission && let Some(Some(log)) = &log_object {
					objects.insert(log.clone());
				}
				if output_has_subtree_permission && let Some(output_objects) = &output_objects {
					objects.extend(output_objects.iter().cloned());
				}
				let destination = id.clone().into();
				let roots = Self::finished_process_objects(&token_data)
					.into_iter()
					.filter(|root| !objects.contains(&root.node))
					.map(|root| root.map(Into::into));
				let items = self.create_capture_permissions_batch_items(
					destination,
					None,
					roots,
					self.context.principal.clone(),
					now,
				)?;
				(objects, items)
			},
			ObjectPermissions::Capture => {
				let destination = id.clone().into();
				let roots = Self::finished_process_objects(&token_data)
					.into_iter()
					.map(|root| root.map(Into::into));
				let source = tg::Principal::Process(id.clone());
				let items = self.create_capture_permissions_batch_items(
					destination,
					None,
					roots,
					source,
					now,
				)?;
				(BTreeSet::new(), items)
			},
		};
		for object in subtree_objects {
			let arg = tangram_index::permission::put::Arg {
				created_at: now,
				creator: Some(tg::Principal::Process(id.clone())),
				permissions: tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)
				.into(),
				resource: object.into(),
				source: tangram_index::permission::Source::Direct { expires_at: None },
				subject: tg::authorization::Subject::Process(id.clone()),
				time_to_touch: None,
				version: None,
			};
			put_object_permissions.push(tangram_index::batch::Item::PutPermission(arg));
		}
		let data = store_data.then(|| arg.data.clone());
		let mut put_process_arg = tangram_index::process::put::Arg {
			cached: false,
			children,
			command: Some(
				arg.data
					.command
					.objects()
					.into_iter()
					.map(|object| object.node)
					.collect(),
			),
			command_id: arg.data.command.command_id()?.into(),
			data,
			error: Some(error_objects),
			id: id.clone(),
			location,
			log: log_object,
			metadata: tg::process::Metadata::default(),
			options: tg::referent::Options::default(),
			output: Some(output_objects),
			parent: None,
			permissions: Vec::new(),
			principal: principal.clone(),
			sandbox: None,
			storage: tg::process::storage::Set::NODE,
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};
		let permission_expires_at = now
			+ self
				.server
				.config
				.process
				.permission_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		let permission_subject = match &self.context.principal {
			tg::Principal::Anonymous => Some(tg::authorization::Subject::Public),
			tg::Principal::Root => None,
			principal => Some(principal.try_to_subject()?),
		};
		let put_permission =
			permission_subject.map(|permission_subject| tangram_index::permission::put::Arg {
				created_at: now,
				creator: Some(self.context.principal.clone()),
				permissions: tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Node,
				)
				.into(),
				resource: id.clone().into(),
				source: tangram_index::permission::Source::Direct {
					expires_at: Some(permission_expires_at),
				},
				subject: permission_subject,
				time_to_touch: Some(self.server.config.process.permission_time_to_touch),
				version: None,
			});
		put_process_arg.permissions.extend(put_permission);
		let account = self.usage_account(&self.context.principal).await?;

		// Put the process in the index.
		let arg = tangram_index::batch::Arg {
			items: std::iter::once(tangram_index::batch::Item::PutProcess(put_process_arg))
				.chain(put_object_permissions)
				.chain(account.map(|account| {
					tangram_index::batch::Item::PutAccountProcess(
						tangram_index::usage::storage::put::ProcessArg {
							account,
							process: id.clone(),
							touched_at: now,
						},
					)
				}))
				.collect(),
		};
		let result = if defer_index {
			self.server.index_batch(arg).await
		} else {
			self.server
				.index
				.batch(arg)
				.await
				.and_then(std::convert::identity)
		};
		result
			.map_err(|error| tg::error!(!error, %id, "failed to put the process in the index"))?;

		// Only issue proofs for permissions the caller actually holds.
		let tokens = if principal == tg::Principal::Process(id.clone()) && defer_index {
			tg::authorization::Tokens::default()
		} else {
			let permissions = self.process_permission_for_data(&token_data);
			let authorization = self
				.authorize(
					id.clone(),
					tg::authorization::permission::Set::Process(permissions),
				)
				.await?
				.check_exhaustion()?;
			let permissions = permissions
				.iter()
				.map(tg::authorization::Permission::Process)
				.filter(|permission| authorization.permissions.contains(*permission))
				.collect::<Vec<_>>();
			let expires_at = authorization
				.expires_at
				.map_or(permission_expires_at, |expires_at| {
					expires_at.min(permission_expires_at)
				});
			let token =
				if !authorization
					.permissions
					.contains(tg::authorization::Permission::Process(
						tg::authorization::permission::process::Permission::Node,
					)) || permissions.is_empty()
				{
					None
				} else {
					self.create_token(id.clone().into(), permissions, expires_at)?
				};
			tg::authorization::Tokens::with_authorization(token)
		};

		Ok(tg::process::put::Output { tokens })
	}

	async fn put_process_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
		region: String,
	) -> tg::Result<tg::process::put::Output> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::put::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client
			.put_process(id, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to put the process"))?;
		Ok(output)
	}

	pub(crate) fn validate_process_data(data: &tg::process::Data) -> tg::Result<()> {
		if data.status != tg::process::Status::Finished {
			return Err(tg::error!("expected a finished process"));
		}
		if let Some(children) = &data.children {
			let mut ids = BTreeSet::new();
			for child in children {
				if !ids.insert(&child.process.node) {
					return Err(tg::error!("the process children must be unique"));
				}
			}
		}

		Ok(())
	}

	async fn put_process_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<(tg::process::put::Output, bool)> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let trusted = client.trusted();
		let location = region.as_deref().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|region| {
				tg::Location::Local(tg::location::Local {
					region: Some(region.to_owned()),
				})
			},
		);
		let arg = tg::process::put::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client
			.put_process(id, arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to put the process"))?;
		Ok((output, trusted))
	}

	pub(crate) async fn put_process_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Put the process.
		let output = Box::pin(self.put_process(&id, arg))
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the process"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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
