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

pub(super) struct Authorization {
	pub(super) error_grants_subtree: bool,
	pub(super) output_grants_subtree: bool,
}

impl Session {
	pub(crate) async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<tg::process::put::Output> {
		let location = self.server.location(arg.location.as_ref())?;

		let mut output = match location.clone() {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.put_process_local(id, arg, false).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.put_process_region(id, arg, region).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.put_process_remote(id, arg, remote, region).await?,
		};
		self.update_tokens_for_location(&mut output.tokens, &location)?;

		Ok(output)
	}

	pub(crate) async fn put_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
		enqueue_log_compaction: bool,
	) -> tg::Result<tg::process::put::Output> {
		Self::validate_process_data(&arg.data)?;
		let authorization = self.authorize_process_data(&arg.data).await?;
		let output = self
			.put_process_local_inner(id, arg, authorization, enqueue_log_compaction)
			.await?;

		Ok(output)
	}

	pub(super) async fn authorize_process_data(
		&self,
		data: &tg::process::Data,
	) -> tg::Result<Authorization> {
		let mut objects = Vec::new();
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
		if let Some(output) = &data.output {
			output.children_with_tokens(&mut objects);
		}
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let permissions = tg::authorization::permission::Set::from_permission(permission);
		let authorizations = self
			.authorize_batch(objects.into_iter().map(|object| (object, permissions)))
			.await?;
		let (error_authorizations, output_authorizations) =
			authorizations.split_at(error_object_count);
		let grants_subtree = |authorizations: &[Option<tg::authorization::permission::Set>]| {
			authorizations.iter().all(|authorization| {
				authorization.is_some_and(|permissions| permissions.contains(permission))
			})
		};
		let error_grants_subtree = grants_subtree(error_authorizations);
		let output_grants_subtree = grants_subtree(output_authorizations);
		let authorization = Authorization {
			error_grants_subtree,
			output_grants_subtree,
		};

		Ok(authorization)
	}

	pub(super) async fn put_process_local_inner(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::put::Arg,
		authorization: Authorization,
		enqueue_log_compaction: bool,
	) -> tg::Result<tg::process::put::Output> {
		let Authorization {
			error_grants_subtree,
			output_grants_subtree,
		} = authorization;
		let now = self.server.clock.unix_timestamp()?;
		let token_data = arg.data.clone();

		arg.data = arg.data.without_location_and_tokens();

		// Create the index arguments.
		let children = arg.data.children.clone();
		let error = if error_grants_subtree {
			arg.data.error.as_ref().map(|error| match error {
				tg::Either::Left(data) => {
					let mut children = BTreeSet::new();
					data.children(&mut children);
					children.into_iter().collect::<Vec<_>>()
				},
				tg::Either::Right(id) => {
					let id = id.node.clone().into();
					vec![id]
				},
			})
		} else {
			Some(Vec::new())
		};
		let mut output = BTreeSet::new();
		if let Some(data) = &arg.data.output {
			data.children(&mut output);
		}
		let output = if output_grants_subtree {
			arg.data
				.output
				.as_ref()
				.map(|_| output.into_iter().collect::<Vec<_>>())
		} else {
			Some(Vec::new())
		};
		let log_needs_compaction = Self::process_log_needs_compaction(&arg.data);
		let log = (!log_needs_compaction).then(|| arg.data.log.clone().map(|log| log.node.into()));
		let enqueue_log_compaction = enqueue_log_compaction && log_needs_compaction;
		let put_process_arg = tangram_index::process::put::Arg {
			cached: false,
			children,
			command: arg.data.command.node.clone().into(),
			data: Some(arg.data.clone()),
			error: Some(error),
			id: id.clone(),
			log,
			metadata: tg::process::Metadata::default(),
			options: tg::referent::Options::default(),
			output: Some(output),
			parent: None,
			sandbox: Some(arg.data.sandbox.clone()),
			stored: tangram_index::process::Stored::default(),
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};
		let grant_expires_at = now
			+ self
				.server
				.config
				.process
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		let grant_subject = match &self.context.principal {
			tg::Principal::Anonymous => Some(tg::authorization::Subject::Public),
			tg::Principal::Root => None,
			principal => Some(principal.try_to_subject()?),
		};
		let put_grant = grant_subject.map(|grant_subject| tangram_index::grant::put::Arg {
			created_at: now,
			creator: Some(self.context.principal.clone()),
			expires_at: Some(grant_expires_at),
			permissions: tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Node,
			)
			.into(),
			subject: grant_subject,
			resource: id.clone().into(),
			time_to_touch: Some(self.server.config.process.grant_time_to_touch),
		});
		let account = self.usage_account(&self.context.principal).await?;

		// Put the process in the index.
		self.server
			.index
			.batch(tangram_index::batch::Arg {
				items: std::iter::once(tangram_index::batch::Item::PutProcess(put_process_arg))
					.chain(put_grant.map(tangram_index::batch::Item::PutGrant))
					.chain(
						enqueue_log_compaction
							.then(|| tangram_index::batch::Item::EnqueueLogCompaction(id.clone())),
					)
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
			})
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the process in the index"))?;
		if enqueue_log_compaction {
			self.server.spawn_publish_log_compaction_notification_task();
		}
		let permission = self.process_permission_for_data(&token_data);
		let tokens = tg::authorization::Tokens::with_local(
			self.create_token(
				id.clone().into(),
				permission
					.iter()
					.map(tg::authorization::Permission::Process)
					.collect(),
				grant_expires_at,
			)?,
		);

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
	) -> tg::Result<tg::process::put::Output> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, %id, "failed to get the remote client"),
		)?;
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
		Ok(output)
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
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

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
