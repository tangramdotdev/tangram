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
		let permission = tg::grant::Permission::Tag(tg::grant::permission::tag::Permission::Write);
		let authorized = self
			.authorize(
				tg::grant::Resource::Specifier(arg.specifier.clone()),
				permission,
			)
			.await?;
		if authorized.is_some_and(|permissions| !permissions.contains(permission)) {
			return Err(tg::error!("unauthorized"));
		}
		let permissions = self.recorded_tag_permissions(&arg.item).await?;
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let permissions = permissions.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let data = session
						.put_tag_with_transaction(transaction, arg, permissions, &mut batch)
						.await?;
					batch.items.push(tangram_index::batch::Item::PutTag(
						tangram_index::tag::put::Arg {
							id: data.id,
							item: match data.item {
								tg::tag::data::Item::Object(id) => tg::Either::Left(id),
								tg::tag::data::Item::Process(id) => tg::Either::Right(id),
							},
							name: data.name,
							parent: data.parent,
							permissions: data.permissions,
							specifier: data.specifier,
						},
					));
					session
						.server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;
		Ok(())
	}

	pub(crate) async fn put_tag_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
		permissions: Vec<tg::grant::Permission>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::tag::Data> {
		let parent = if arg.parents {
			self.create_parent_groups_with_transaction(transaction, &arg.specifier, batch)
				.await?
		} else {
			Self::resolve_parent_for_specifier_with_transaction(transaction, &arg.specifier).await?
		};
		let existing =
			Self::try_get_id_for_specifier_with_transaction(transaction, &arg.specifier).await?;
		let item = Self::tag_item_to_string(&arg.item);
		let permissions_json = serde_json::to_string(&permissions)
			.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
		let (id, permissions) = if let Some(id) = existing {
			let Ok(id) = tg::tag::Id::try_from(id) else {
				return Err(tg::error!("specifier is already in use"));
			};
			let p = transaction.p();
			// Keep the recorded permissions when the item is unchanged, and record the new permissions when the item is replaced.
			let statement = formatdoc!(
				"
					update tags
					set permissions = case when item = {p}1 then permissions else {p}3 end,
						item = {p}1
					where id = {p}2;
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![item.clone(), id.to_string(), permissions_json],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
			let row = transaction
				.query_one_into::<Row>(statement.into(), db::params![id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			let permissions = serde_json::from_str(&row.permissions)
				.map_err(|error| tg::error!(!error, "failed to deserialize the permissions"))?;
			(id, permissions)
		} else {
			let id = tg::tag::Id::new();
			Self::insert_specifier_with_transaction(
				transaction,
				&id.clone().into(),
				&arg.specifier,
			)
			.await?;
			let name = arg.specifier.name().to_owned();
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into tags (id, name, parent, item, permissions)
					values ({p}1, {p}2, {p}3, {p}4, {p}5);
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![
						id.to_string(),
						name,
						parent.as_ref().map(ToString::to_string),
						item.clone(),
						permissions_json
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if arg.public {
				let arg = tg::grant::create::Arg {
					principal: tg::grant::Principal::Public.into(),
					permissions: tg::Either::Left(
						tg::grant::Permission::Tag(tg::grant::permission::tag::Permission::Read)
							.into(),
					),
					resource: tg::Referent::with_item(tg::grant::Resource::Id(id.clone().into())),
				};
				self.create_grant_with_transaction(transaction, arg, batch)
					.await?;
			}
			if let Some(principal) = self.write_user_grant_principal() {
				let arg = tg::grant::create::Arg {
					principal: principal.into(),
					permissions: tg::Either::Left(
						tg::grant::Permission::Tag(tg::grant::permission::tag::Permission::Admin)
							.into(),
					),
					resource: tg::Referent::with_item(tg::grant::Resource::Id(id.clone().into())),
				};
				self.create_grant_with_transaction(transaction, arg, batch)
					.await?;
			}
			(id, permissions)
		};
		Ok(tg::tag::Data {
			id,
			item: arg.item,
			name: arg.specifier.name().to_owned(),
			parent,
			permissions,
			specifier: arg.specifier,
		})
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

	pub(crate) fn write_user_grant_principal(&self) -> Option<tg::grant::Principal> {
		match &self.context.principal {
			tg::Principal::User(user) => Some(tg::grant::Principal::User(user.clone())),
			_ => None,
		}
	}
}
