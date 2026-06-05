use {
	crate::{Session, context::Authentication, database::Transaction, tag::tag_item_to_string},
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
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
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
		let session = self.clone();
		let data = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let data = session.put_tag_with_transaction(transaction, arg).await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(data))
				}
				.boxed()
			})
			.await?;
		let arg = tangram_index::PutTagArg {
			tag: data.specifier,
			item: match data.item {
				tg::tag::data::Item::Object(id) => tg::Either::Left(id),
				tg::tag::data::Item::Process(id) => tg::Either::Right(id),
			},
		};
		self.server
			.index
			.put_tags(&[arg])
			.await
			.map_err(|error| tg::error!(!error, "failed to index the tag"))?;
		Ok(())
	}

	pub(crate) async fn put_tag_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
	) -> tg::Result<tg::tag::Data> {
		let parent = self
			.ensure_parent_for_specifier(transaction, &arg.specifier)
			.await?;
		let existing =
			Self::try_get_node_by_specifier_with_transaction(transaction, &arg.specifier).await?;
		let item = tag_item_to_string(&arg.item);
		let node = if let Some(node) = existing {
			if node.kind != tg::id::Kind::Tag {
				return Err(tg::error!("specifier is already in use"));
			}
			let p = transaction.p();
			let statement = formatdoc!(
				"
					update tags
					set item = {p}1
					where id = {p}2 and ({p}3 or item = {p}1);
				"
			);
			let n = transaction
				.execute(
					statement.into(),
					db::params![item.clone(), node.id.to_string(), arg.force],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if n == 0 {
				return Err(tg::error!("the tag already exists with a different item"));
			}
			node
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
					insert into tags (id, name, parent, item)
					values ({p}1, {p}2, {p}3, {p}4);
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![
						id.to_string(),
						node.name.clone(),
						node.parent.as_ref().map(ToString::to_string),
						item.clone()
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if arg.public {
				let public = Self::ensure_public_group_with_transaction(transaction).await?;
				let arg = tg::grant::create::Arg {
					principal: tg::grant::Principal::Group(public).into(),
					permission: tg::grant::Permission::Read,
					resource: tg::grant::Resource::Id(id.clone().into()),
				};
				self.create_grant_with_transaction(transaction, arg).await?;
			}
			if let Some(principal) = self.write_user_grant_principal() {
				let arg = tg::grant::create::Arg {
					principal: principal.into(),
					permission: tg::grant::Permission::Admin,
					resource: tg::grant::Resource::Id(id.clone().into()),
				};
				self.create_grant_with_transaction(transaction, arg).await?;
			}
			node
		};
		Ok(tg::tag::Data {
			id: node.id.try_into()?,
			item: arg.item,
			name: node.name,
			parent: node.parent,
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
		self.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| tg::grant::Principal::User(user.id.clone()))
	}
}

impl Session {
	async fn ensure_parent_for_specifier(
		&self,
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<tg::Id>> {
		let components = specifier.components().collect::<Vec<_>>();
		if components.is_empty() {
			return Err(tg::error!("invalid specifier"));
		}
		if components.len() == 1 {
			return Ok(None);
		}
		let mut parent = None;
		let mut node = None;
		for index in 0..components.len() - 1 {
			let specifier = tg::Specifier::with_components(
				components[..=index]
					.iter()
					.map(|component| tg::specifier::Component::new((*component).to_owned())),
			);
			if let Some(existing) =
				Self::try_get_node_by_specifier_with_transaction(transaction, &specifier).await?
			{
				if existing.kind == tg::id::Kind::Tag {
					return Err(tg::error!("specifier is already in use"));
				}
				parent = Some(existing.id.clone());
				node = Some(existing);
				continue;
			}
			let created = self
				.create_group_node_with_transaction(transaction, &specifier, parent.as_ref())
				.await?;
			parent = Some(created.id.clone());
			node = Some(created);
		}
		Ok(node.map(|node| node.id))
	}
}
