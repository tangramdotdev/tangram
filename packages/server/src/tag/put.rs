use {
	crate::{Session, database::Transaction},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
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
			tg::Location::Local(_) => self.put_tag_local(arg).await,
			tg::Location::Remote(remote) => self.put_tag_remote(arg, remote).await,
		}
	}

	async fn put_tag_local(&self, arg: tg::tag::put::Arg) -> tg::Result<()> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let permission = tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		);
		let authorized = self
			.authorize(
				tg::Selector::<tg::Id>::Specifier(arg.specifier.clone()),
				permission,
			)
			.await?;
		if authorized.is_some_and(|permissions| !permissions.contains(permission)) {
			return Err(tg::error!("unauthorized"));
		}
		self.pull_ancestors(&arg.specifier, arg.ancestors.pull)
			.await?;
		let permissions = self.recorded_tag_target_permissions(&arg.target).await?;
		let touched_at = self.server.clock.unix_timestamp()?;
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let permissions = permissions.clone();
				let session = session.clone();
				async move {
					session
						.put_tag_local_with_transaction(transaction, arg, permissions, touched_at)
						.await
				}
				.boxed()
			})
			.await?;
		Ok(())
	}

	async fn put_tag_local_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
		permissions: Vec<tg::authorization::Permission>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
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

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn put_tag_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
		permissions: Vec<tg::authorization::Permission>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<tg::tag::Data, crate::database::Error>> {
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
		let existing =
			match Self::try_get_id_for_specifier_with_transaction(transaction, &arg.specifier)
				.await?
			{
				ControlFlow::Break(existing) => existing,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let target = Self::tag_target_to_string(&arg.target);
		let permissions_json = serde_json::to_string(&permissions)
			.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
		let (id, permissions) = if let Some(id) = existing {
			let Ok(id) = tg::tag::Id::try_from(id) else {
				return Err(tg::error!("specifier is already in use"));
			};
			let p = transaction.p();
			// Keep the recorded permissions when the target is unchanged, and record the new permissions when the target is replaced.
			let statement = formatdoc!(
				"
					update tags
					set permissions = case when target = {p}1 then permissions else {p}3 end,
						target = {p}1
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
			#[derive(db::row::Deserialize)]
			struct Row {
				permissions: String,
			}
			let statement = formatdoc!(
				"
					select permissions
					from tags
					where id = {p}1;
				"
			);
			let result = transaction
				.query_one_into::<Row>(statement.into(), db::params![id.to_string()])
				.await;
			let row = crate::database::retry!(result, "failed to execute the statement");
			let permissions = serde_json::from_str(&row.permissions)
				.map_err(|error| tg::error!(!error, "failed to deserialize the permissions"))?;
			(id, permissions)
		} else {
			let id = tg::tag::Id::new();
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
