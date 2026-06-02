use {
	crate::{Session, context::Authentication},
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

impl Session {
	pub(crate) async fn create_namespace(&self, arg: tg::namespace::create::Arg) -> tg::Result<()> {
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());
		let arg = arg.clone();
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let created_by = created_by.clone();
				let session = session.clone();
				async move {
					session
						.create_namespace_inner_with_transaction(
							transaction,
							&arg,
							created_by.as_ref(),
						)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to create the namespace"))
	}

	async fn create_namespace_inner_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::namespace::create::Arg,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let namespace = &arg.namespace;
		let namespace_id = match self
			.create_namespace_with_transaction(transaction, namespace, created_by)
			.await?
		{
			ControlFlow::Break(namespace_id) => namespace_id,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if arg.all {
			match Self::create_namespace_grant_for_all_with_transaction(
				transaction,
				namespace,
				namespace_id,
				created_by,
			)
			.await?
			{
				ControlFlow::Break(_) => {},
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		Ok(ControlFlow::Break(()))
	}

	async fn create_namespace_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		namespace: &tg::Namespace,
		created_by: Option<&tg::user::Id>,
	) -> tg::Result<ControlFlow<i64, crate::database::Error>> {
		if namespace.is_root() {
			return Ok(ControlFlow::Break(0));
		}

		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}

		let mut components = Vec::new();
		let mut parent_created = false;
		let mut namespace_id = 0;
		let mut target_created = false;
		for component in namespace.components() {
			components.push(component.to_owned());
			let current = tg::Namespace::with_components(components.clone());
			if let Some(current_id) =
				Self::try_get_namespace_id_with_transaction(transaction, &current).await?
			{
				namespace_id = current_id;
				parent_created = false;
				continue;
			}
			if Self::namespace_path_has_tag_with_transaction(transaction, &current).await? {
				return Err(tg::error!("a tag exists at the namespace path"));
			}
			if components.len() == 1 {
				match &self.context.authentication {
					Some(Authentication::Root | Authentication::User(_)) => {},
					_ => return Err(tg::error!("unauthorized")),
				}
			} else if !parent_created {
				let parent = tg::Namespace::with_components(
					components[..components.len() - 1].iter().cloned(),
				);
				match &self.context.authentication {
					Some(Authentication::Root) => {},
					Some(Authentication::User(user))
						if Self::user_has_namespace_permission_with_transaction(
							transaction,
							&user.id,
							&parent,
							tg::Permission::Write,
						)
						.await? => {},
					_ => return Err(tg::error!("unauthorized")),
				}
			}
			namespace_id = match Self::get_or_create_namespace_with_transaction(
				transaction,
				&current,
			)
			.await?
			{
				ControlFlow::Break(namespace_id) => namespace_id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			if let Some(user) = created_by {
				match Self::create_namespace_grant_for_user_with_transaction(
					transaction,
					&current,
					namespace_id,
					user,
					tg::Permission::Admin,
					created_by,
				)
				.await?
				{
					ControlFlow::Break(_) => {},
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
			if current == *namespace {
				target_created = true;
			}
			parent_created = true;
		}

		if !target_created {
			match (
				&self.context.authentication,
				Self::parent_namespace(namespace),
			) {
				(Some(Authentication::Root), _) => {},
				(Some(Authentication::User(user)), Some(parent))
					if Self::user_has_namespace_permission_with_transaction(
						transaction,
						&user.id,
						&parent,
						tg::Permission::Write,
					)
					.await? => {},
				(Some(Authentication::User(user)), None)
					if Self::user_has_namespace_permission_with_transaction(
						transaction,
						&user.id,
						namespace,
						tg::Permission::Write,
					)
					.await? => {},
				_ => return Err(tg::error!("unauthorized")),
			}
		}

		if namespace_id == 0 {
			return Err(tg::error!("failed to create the namespace"));
		}

		Ok(ControlFlow::Break(namespace_id))
	}

	pub(crate) async fn create_namespace_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg: tg::namespace::create::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.create_namespace(arg.clone()).await.map_err(
			|error| tg::error!(!error, namespace = %arg.namespace, "failed to create the namespace"),
		)?;
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
