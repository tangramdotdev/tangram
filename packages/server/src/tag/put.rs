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
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn put_tag(&self, arg: tg::tag::put::Arg) -> tg::Result<()> {
		if matches!(self.context.principal, Some(tg::Principal::Process(_))) {
			return Err(tg::error!("unauthorized"));
		}
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
		if self.context.principal.is_none() {
			return Err(tg::error!("unauthorized"));
		}
		let authorized = self
			.authorize(
				tg::grant::Resource::Specifier(arg.specifier.clone()),
				tg::grant::Permission::Write,
			)
			.await?;
		if authorized.is_some_and(|permissions| !permissions.contains(tg::grant::Permission::Write))
		{
			return Err(tg::error!("unauthorized"));
		}
		let permissions = self.recorded_tag_permissions(&arg.item).await?;
		let session = self.clone();
		let (data, mut batch) = self
			.server
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
					Ok::<_, crate::database::Error>(ControlFlow::Break((data, batch)))
				}
				.boxed()
			})
			.await?;
		batch.put_tags.push(tangram_index::tag::put::Arg {
			id: data.id,
			item: match data.item {
				tg::tag::data::Item::Object(id) => tg::Either::Left(id),
				tg::tag::data::Item::Process(id) => tg::Either::Right(id),
			},
			name: data.name,
			parent: data.parent,
			permissions: data.permissions,
			specifier: data.specifier,
		});
		if !batch.is_empty() {
			self.server
				.index
				.batch(batch)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the tag"))?;
		}
		Ok(())
	}

	pub(crate) async fn put_tag_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
		permissions: Vec<tg::grant::Permission>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::tag::Data> {
		let parent = self
			.ensure_parent_for_specifier(transaction, &arg.specifier, batch)
			.await?;
		let existing =
			Self::try_get_node_by_specifier_with_transaction(transaction, &arg.specifier).await?;
		let item = Self::tag_item_to_string(&arg.item);
		let permissions_json = serde_json::to_string(&permissions)
			.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
		let (node, permissions) = if let Some(node) = existing {
			if node.kind != tg::id::Kind::Tag {
				return Err(tg::error!("specifier is already in use"));
			}
			let p = transaction.p();
			// Keep the recorded permissions when the item is unchanged, and record the new permissions when the item is replaced.
			let statement = formatdoc!(
				"
					update tags
					set permissions = case when item = {p}1 then permissions else {p}4 end,
						item = {p}1
					where id = {p}2 and ({p}3 or item = {p}1);
				"
			);
			let n = transaction
				.execute(
					statement.into(),
					db::params![
						item.clone(),
						node.id.to_string(),
						arg.force,
						permissions_json
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if n == 0 {
				return Err(tg::error!("the tag already exists with a different item"));
			}
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
				.query_one_into::<Row>(statement.into(), db::params![node.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			let permissions = serde_json::from_str(&row.permissions)
				.map_err(|error| tg::error!(!error, "failed to deserialize the permissions"))?;
			(node, permissions)
		} else {
			let id = tg::tag::Id::new();
			let node = Self::create_node_with_transaction(
				transaction,
				&id.clone().into(),
				tg::id::Kind::Tag,
				&arg.specifier,
				parent.as_ref(),
			)
			.await?;
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
						node.name.clone(),
						node.parent.as_ref().map(ToString::to_string),
						item.clone(),
						permissions_json
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if arg.public {
				let arg = tg::grant::create::Arg {
					principal: tg::grant::Principal::Public.into(),
					permissions: tg::grant::Permission::Read.into(),
					resource: tg::grant::Resource::Id(id.clone().into()),
				};
				self.create_grant_with_transaction(transaction, arg, batch)
					.await?;
			}
			if let Some(principal) = self.write_user_grant_principal() {
				let arg = tg::grant::create::Arg {
					principal: principal.into(),
					permissions: tg::grant::Permission::Admin.into(),
					resource: tg::grant::Resource::Id(id.clone().into()),
				};
				self.create_grant_with_transaction(transaction, arg, batch)
					.await?;
			}
			(node, permissions)
		};
		Ok(tg::tag::Data {
			id: node.id.try_into()?,
			item: arg.item,
			name: node.name,
			parent: node.parent,
			permissions,
			specifier: node.specifier,
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
		match self.context.principal.as_ref() {
			Some(tg::Principal::User(user)) => Some(tg::grant::Principal::User(user.clone())),
			_ => None,
		}
	}
}

impl Session {
	async fn ensure_parent_for_specifier(
		&self,
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Option<tg::Id>> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let Some(_) = specifier.parent() else {
			return Ok(None);
		};
		let mut parent = None;
		let mut node = None;
		for ancestor in specifier.ancestors() {
			if let Some(existing) =
				Self::try_get_node_by_specifier_with_transaction(transaction, &ancestor).await?
			{
				if existing.kind == tg::id::Kind::Tag {
					return Err(tg::error!("specifier is already in use"));
				}
				parent = Some(existing.id.clone());
				node = Some(existing);
				continue;
			}
			let created = self
				.create_group_node_with_transaction(transaction, &ancestor, parent.as_ref(), batch)
				.await?;
			parent = Some(created.id.clone());
			node = Some(created);
		}
		Ok(node.map(|node| node.id))
	}
}
