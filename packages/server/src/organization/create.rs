use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.create_organization_local(arg).await,
			tg::Location::Remote(remote) => self.create_organization_remote(arg, remote).await,
		}
	}

	async fn create_organization_local(
		&self,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let session = self.clone();
		let (output, batch) = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let organization = session
						.create_organization_with_transaction(transaction, arg, &mut batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break((
						tg::organization::create::Output { organization },
						batch,
					)))
				}
				.boxed()
			})
			.await?;
		if !batch.is_empty() {
			self.server
				.index
				.batch(batch)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the organization"))?;
		}
		Ok(output)
	}

	async fn create_organization_remote(
		&self,
		mut arg: tg::organization::create::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::organization::create::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.create_organization(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to create the organization"),
		)
	}

	async fn create_organization_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::organization::create::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::Organization> {
		if arg.specifier.components().count() != 1 {
			return Err(tg::error!("invalid organization specifier"));
		}
		if Self::try_get_node_by_specifier_with_transaction(transaction, &arg.specifier)
			.await?
			.is_some()
		{
			return Err(tg::error!("specifier is already in use"));
		}
		let id = tg::organization::Id::new();
		let node = Self::create_node_with_transaction(
			transaction,
			&id.clone().into(),
			tg::id::Kind::Organization,
			&arg.specifier,
			None,
		)
		.await?;
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into organizations (id, name)
				values ({p}1, {p}2);
			"
		);
		transaction
			.execute(
				statement.into(),
				db::params![id.to_string(), node.name.clone()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		batch
			.put_organizations
			.push(tangram_index::organization::put::Arg {
				id: id.clone(),
				specifier: node.specifier.clone(),
			});
		if !matches!(
			self.context.principal,
			tg::Principal::Anonymous | tg::Principal::Root
		) {
			let principal = &self.context.principal;
			let arg = tg::grant::create::Arg {
				principal: principal.try_to_grant_principal()?.into(),
				permissions: tg::Either::Left(
					tg::grant::Permission::Organization(
						tg::grant::permission::organization::Permission::Admin,
					)
					.into(),
				),
				resource: tg::grant::Resource::Id(id.clone().into()),
			};
			self.create_grant_with_transaction(transaction, arg, batch)
				.await?;
		}
		Ok(tg::Organization {
			id: node.id.try_into()?,
			name: node.name,
			specifier: node.specifier,
		})
	}

	pub(crate) async fn create_organization_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_organization(arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}
}
