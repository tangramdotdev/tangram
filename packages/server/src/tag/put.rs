use {
	crate::{Session, database::Transaction},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
};

impl Session {
	pub(crate) async fn put_tag(&self, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.verify_request_with_network_access()?;
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) if !self.server.is_primary_region() => {
				self.put_tag_primary_region(arg).await
			},
			tg::Location::Local(_) => self.put_tag_local(arg).await,
			tg::Location::Remote(remote) => self.put_tag_remote(arg, remote).await,
		}
	}

	async fn put_tag_local(&self, arg: tg::tag::put::Arg) -> tg::Result<()> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		self.pull_ancestors(&arg.specifier, arg.ancestors.pull)
			.await?;
		let permissions = self.recorded_tag_target_permissions(&arg.target).await?;
		let touched_at = self.server.clock.unix_timestamp()?;
		let options = tangram_futures::retry::Options::default();
		let session = self.clone();
		tangram_futures::retry(&options, || {
			let arg = arg.clone();
			let permissions = permissions.clone();
			let session = session.clone();
			async move {
				match session
					.put_tag_local_attempt(arg, permissions, touched_at)
					.await?
				{
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		self.checkout_index_barrier().await?;
		Ok(())
	}

	async fn put_tag_local_attempt(
		&self,
		arg: tg::tag::put::Arg,
		permissions: Vec<tg::authorization::Permission>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<()>> {
		let specifiers = std::slice::from_ref(&arg.specifier);
		let ids_by_specifier = self
			.try_get_ids_and_ancestors_for_specifiers(specifiers)
			.await?;
		self.authorize_tag_puts(specifiers, arg.force, &ids_by_specifier)
			.await?;
		crate::checkpoint!(self.server, "tag.put.authorized", specifier = %arg.specifier).await;
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let ids_by_specifier = ids_by_specifier.clone();
				let permissions = permissions.clone();
				let session = session.clone();
				async move {
					session
						.put_tag_local_with_transaction(
							transaction,
							arg,
							&ids_by_specifier,
							permissions,
							touched_at,
						)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(output)
	}

	pub(crate) async fn authorize_tag_puts(
		&self,
		specifiers: &[tg::Specifier],
		force: bool,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<()> {
		let batch_size = self.server.config.sync.get.database.batch_size;

		// Collect the required write and replacement authorizations.
		let mut authorizations = Vec::new();
		for specifier in specifiers {
			let existing = ids_by_specifier
				.get(specifier)
				.and_then(std::option::Option::as_ref);
			let parent = specifier
				.parent()
				.and_then(|specifier| ids_by_specifier.get(&specifier))
				.and_then(std::option::Option::as_ref);
			match existing {
				Some(id) if id.kind() == tg::id::Kind::Tag => {
					let permission = tg::authorization::Permission::Tag(
						tg::authorization::permission::tag::Permission::Write,
					);
					let permissions =
						tg::authorization::permission::Set::from_permission(permission);
					let resource = tg::Selector::<tg::Id>::Id(id.clone());
					authorizations.push((resource, false, permissions));
				},
				Some(id) => {
					if !force {
						return Err(tg::error!("specifier is already in use"));
					}
					let permission = Self::delete_permission_for_named_node(id)?;
					let permissions =
						tg::authorization::permission::Set::from_permission(permission);
					let resource = tg::Selector::<tg::Id>::Id(id.clone());
					authorizations.push((resource, false, permissions));
					if let Some(parent) = parent {
						let permission = Self::write_permission_for_resource(parent)?;
						let permissions =
							tg::authorization::permission::Set::from_permission(permission);
						let resource = tg::Selector::<tg::Id>::Id(parent.clone());
						authorizations.push((resource, false, permissions));
					}
				},
				None => {
					let permission = tg::authorization::Permission::Tag(
						tg::authorization::permission::tag::Permission::Write,
					);
					let permissions =
						tg::authorization::permission::Set::from_permission(permission);
					let resource = tg::Selector::<tg::Id>::Specifier(specifier.clone());
					authorizations.push((resource, true, permissions));
				},
			}
		}

		// Authorize the operations in batches.
		for authorizations in authorizations.chunks(batch_size) {
			let args = authorizations
				.iter()
				.map(|(resource, _, permissions)| (resource.clone(), *permissions))
				.collect::<Vec<_>>();
			let outputs = self.authorize_batch(args).await?;
			for ((_, allow_unclaimed, permissions), output) in
				std::iter::zip(authorizations, outputs)
			{
				let authorized = match output {
					Some(output) => output.contains(*permissions),
					None => *allow_unclaimed,
				};
				if !authorized {
					return Err(tg::error!("unauthorized"));
				}
			}
		}

		Ok(())
	}

	async fn put_tag_local_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
		permissions: Vec<tg::authorization::Permission>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<ControlFlow<()>, crate::database::Error>> {
		let batch_size = self.server.config.sync.get.database.batch_size;
		match Self::verify_ids_for_specifiers_with_transaction(
			transaction,
			ids_by_specifier,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(true) => (),
			ControlFlow::Break(false) => {
				return Ok(ControlFlow::Break(ControlFlow::Continue(())));
			},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let mut batch = tangram_index::batch::Arg::default();
		let data = match self
			.put_tag_with_transaction(transaction, arg, permissions, &mut batch)
			.await?
		{
			ControlFlow::Break(data) => data,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let account = match self
			.usage_account_for_specifier_with_transaction(transaction, &data.specifier)
			.await?
		{
			ControlFlow::Break(account) => account,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let target = match data.target {
			tg::tag::data::Target::Object(id) => tg::Either::Left(id),
			tg::tag::data::Target::Process(id) => tg::Either::Right(id),
		};
		batch.items.push(tangram_index::batch::Item::PutTag(
			tangram_index::tag::put::Arg {
				account: account.clone(),
				id: data.id,
				name: data.name,
				parent: data.parent,
				permissions: data.permissions,
				specifier: data.specifier,
				target: target.clone(),
			},
		));
		if let Some(account) = account {
			let item = match target {
				tg::Either::Left(object) => tangram_index::batch::Item::PutAccountObject(
					tangram_index::usage::storage::put::ObjectArg {
						account,
						object,
						touched_at,
					},
				),
				tg::Either::Right(process) => tangram_index::batch::Item::PutAccountProcess(
					tangram_index::usage::storage::put::ProcessArg {
						account,
						process,
						touched_at,
					},
				),
			};
			batch.items.push(item);
		}
		match self
			.server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(ControlFlow::Break(())))
	}

	pub(crate) async fn put_tag_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
		permissions: Vec<tg::authorization::Permission>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<tg::tag::Data, crate::database::Error>> {
		let existing =
			match Self::try_get_id_for_specifier_with_transaction(transaction, &arg.specifier)
				.await?
			{
				ControlFlow::Break(existing) => existing,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};

		// Delete a conflicting non-tag node and its subtree.
		let existing = if existing
			.as_ref()
			.is_some_and(|existing| existing.kind() != tg::id::Kind::Tag)
		{
			if !arg.force {
				return Err(tg::error!("specifier is already in use"));
			}
			let existing = existing
				.as_ref()
				.ok_or_else(|| tg::error!("missing the existing named node"))?;
			let batch_size = self.server.config.sync.get.database.batch_size;
			let replaced_ids_and_specifiers = match Self::collect_named_subtrees_with_transaction(
				transaction,
				std::slice::from_ref(existing),
				batch_size,
			)
			.await?
			{
				ControlFlow::Break(ids_and_specifiers) => ids_and_specifiers,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			match self
				.delete_named_nodes_with_transaction(
					transaction,
					&replaced_ids_and_specifiers,
					batch,
					batch_size,
				)
				.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			None
		} else {
			existing
		};

		// Resolve the parent.
		let parent = if arg.ancestors.create {
			match self
				.create_parent_groups_with_transaction(transaction, &arg.specifier, batch)
				.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		} else {
			match Self::resolve_parent_for_specifier_with_transaction(transaction, &arg.specifier)
				.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		};

		// Put the tag.
		let target = Self::tag_target_to_string(&arg.target);
		let (id, permissions) = if let Some(id) = existing {
			let id =
				tg::tag::Id::try_from(id).map_err(|_| tg::error!("specifier is already in use"))?;
			#[derive(db::row::Deserialize)]
			struct Row {
				permissions: String,
				target: String,
			}
			let p = transaction.p();
			let statement = formatdoc!(
				"
					select permissions, target
					from tags
					where id = {p}1;
				"
			);
			let result = transaction
				.query_one_into::<Row>(statement.into(), db::params![id.to_string()])
				.await;
			let row = crate::database::retry!(result, "failed to execute the statement");
			let permissions = if row.target == target {
				serde_json::from_str(&row.permissions)
					.map_err(|error| tg::error!(!error, "failed to deserialize the permissions"))?
			} else if arg.force {
				permissions
			} else {
				return Err(tg::error!("the tag already has a different target"));
			};
			let permissions_json = serde_json::to_string(&permissions)
				.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
			let p = transaction.p();
			let statement = formatdoc!(
				"
					update tags
					set permissions = {p}3, target = {p}1
					where id = {p}2;
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![target.clone(), id.to_string(), permissions_json],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			(id, permissions)
		} else {
			let id = tg::tag::Id::new();
			let permissions_json = serde_json::to_string(&permissions)
				.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
			match Self::insert_specifier_with_transaction(
				transaction,
				&id.clone().into(),
				&arg.specifier,
			)
			.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			let name = arg.specifier.name().to_owned();
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into tags (id, name, parent, target, permissions)
					values ({p}1, {p}2, {p}3, {p}4, {p}5);
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![
						id.to_string(),
						name,
						parent.as_ref().map(ToString::to_string),
						target.clone(),
						permissions_json
					],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			if arg.public {
				let arg = tg::grant::create::Arg {
					subject: tg::authorization::Subject::Public.into(),
					permissions: tg::Either::Left(
						tg::authorization::Permission::Tag(
							tg::authorization::permission::tag::Permission::Read,
						)
						.into(),
					),
					resource: tg::Referent::with_node(tg::Selector::Id(id.clone().into())),
				};
				match self
					.create_grant_with_transaction(transaction, arg, batch)
					.await?
				{
					ControlFlow::Break(_) => (),
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
			if let Some(subject) = self.write_user_grant_subject() {
				let arg = tg::grant::create::Arg {
					subject: subject.into(),
					permissions: tg::Either::Left(
						tg::authorization::Permission::Tag(
							tg::authorization::permission::tag::Permission::Admin,
						)
						.into(),
					),
					resource: tg::Referent::with_node(tg::Selector::Id(id.clone().into())),
				};
				match self
					.create_grant_with_transaction(transaction, arg, batch)
					.await?
				{
					ControlFlow::Break(_) => (),
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
			(id, permissions)
		};
		let data = tg::tag::Data {
			id,
			target: arg.target,
			name: arg.specifier.name().to_owned(),
			parent,
			permissions,
			specifier: arg.specifier,
		};

		Ok(ControlFlow::Break(data))
	}

	async fn put_tag_primary_region(&self, mut arg: tg::tag::put::Arg) -> tg::Result<()> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.put_tag(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the tag in the primary region"))?;

		Ok(())
	}

	async fn put_tag_remote(
		&self,
		mut arg: tg::tag::put::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.put_tag(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to put the tag"))?;
		self.invalidate_remote_cache(&remote.name).await;

		Ok(())
	}

	pub(crate) async fn put_tag_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.put_tag(arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) fn write_user_grant_subject(&self) -> Option<tg::authorization::Subject> {
		match &self.context.principal {
			tg::Principal::User(user) => Some(tg::authorization::Subject::User(user.clone())),
			_ => None,
		}
	}
}
