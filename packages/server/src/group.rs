use {
	crate::{Session, context::Authentication},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

pub mod grants;

impl Session {
	pub(crate) async fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}

		if self
			.context
			.authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
		}
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());
		let namespace = Self::namespace_for_group(&arg.namespace)?;

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;

		let parent = if let Some(parent_namespace) = Self::parent_namespace(&namespace) {
			let Some(parent) =
				Self::try_get_group_by_namespace_with_transaction(&transaction, &parent_namespace)
					.await?
			else {
				return Err(tg::error!("failed to find the parent group"));
			};
			if let Some(user) = &created_by
				&& !Self::user_has_namespace_permission_with_transaction(
					&transaction,
					user,
					&parent_namespace,
					tg::Permission::Admin,
				)
				.await?
			{
				return Err(tg::error!("unauthorized"));
			}
			Some(parent)
		} else {
			None
		};

		let p = transaction.p();
		let namespace_string = namespace.to_string();
		if Self::namespace_in_use_with_transaction(&transaction, &namespace_string).await? {
			return Err(tg::error!("namespace is already in use"));
		}

		let id = tg::group::Id::new();
		let statement = formatdoc!(
			r"
				insert into groups (id, namespace, parent)
				values ({p}1, {p}2, {p}3);
			"
		);
		let params = db::params![
			id.to_string(),
			namespace.to_string(),
			parent.as_ref().map(|group| group.id.to_string()),
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		if let Some(user) = &created_by {
			let statement = formatdoc!(
				r#"
					insert into group_members ("group", "user")
					values ({p}1, {p}2)
					on conflict ("group", "user") do nothing;
				"#
			);
			let params = db::params![id.to_string(), user.to_string()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}

		let namespace_id =
			Self::get_or_create_namespace_with_transaction(&transaction, &namespace).await?;
		Self::create_namespace_grant_for_group_with_transaction(
			&transaction,
			&namespace,
			namespace_id,
			&id,
			tg::Permission::Admin,
			created_by.as_ref(),
		)
		.await?;

		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		Ok(tg::group::create::Output {
			group: tg::Group {
				id,
				namespace,
				parent: parent.map(|group| group.id),
			},
		})
	}

	pub(crate) async fn list_groups(
		&self,
		_arg: tg::group::list::Arg,
	) -> tg::Result<tg::group::list::Output> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		if self
			.context
			.authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			namespace: String,
			parent: Option<String>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::group::Id,
		}

		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let rows = connection
			.query_all_into::<Row>(
				"
					select id, namespace, parent
					from groups
					order by namespace;
				"
				.into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| {
				Ok(tg::Group {
					id: row.id,
					namespace: row
						.namespace
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse the namespace"))?,
					parent: row
						.parent
						.map(|parent| parent.parse())
						.transpose()
						.map_err(|error| {
							tg::error!(!error, "failed to parse the parent group id")
						})?,
				})
			})
			.collect::<tg::Result<_>>()?;
		Ok(tg::group::list::Output { data })
	}

	pub(crate) async fn try_get_group(&self, group: &str) -> tg::Result<Option<tg::Group>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		if self
			.context
			.authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
		}

		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		Self::try_get_group_with_transaction(&transaction, group).await
	}

	pub(crate) async fn try_delete_group(&self, group: &str) -> tg::Result<Option<()>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let authentication = &self.context.authentication;
		if authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
		}

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(group) = Self::try_get_group_with_transaction(&transaction, group).await? else {
			return Ok(None);
		};
		if let Some(Authentication::User(user)) = authentication
			&& !Self::user_has_namespace_permission_with_transaction(
				&transaction,
				&user.id,
				&group.namespace,
				tg::Permission::Admin,
			)
			.await?
		{
			return Err(tg::error!("unauthorized"));
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select 1
				from groups
				where parent = {p}1;
			"
		);
		if transaction
			.query_optional(statement.into(), db::params![group.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.is_some()
		{
			return Err(tg::error!("the group has child groups"));
		}

		for statement in [
			formatdoc!(
				r"
					delete from namespace_grants
					where principal = {p}1;
				"
			),
			formatdoc!(
				r#"
					delete from group_members
					where "group" = {p}1;
				"#
			),
			formatdoc!(
				r"
					delete from groups
					where id = {p}1;
				"
			),
		] {
			transaction
				.execute(statement.into(), db::params![group.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(Some(()))
	}

	pub(crate) async fn list_group_members(
		&self,
		group: &str,
	) -> tg::Result<Option<tg::group::member::list::Output>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let authentication = &self.context.authentication;
		if authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
		}

		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(group) = Self::try_get_group_with_transaction(&transaction, group).await? else {
			return Ok(None);
		};
		if let Some(Authentication::User(user)) = authentication
			&& !Self::user_has_namespace_permission_with_transaction(
				&transaction,
				&user.id,
				&group.namespace,
				tg::Permission::Read,
			)
			.await?
		{
			return Err(tg::error!("unauthorized"));
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			email: Option<String>,
			namespace: Option<String>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select users.id, users.namespace, user_emails.email
				from group_members
				join users on users.id = group_members."user"
				left join user_emails on user_emails."user" = users.id
				where group_members."group" = {p}1
				order by users.namespace, users.id, user_emails.email;
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![group.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut data = Vec::<tg::User>::new();
		for row in rows {
			if let Some(user) = data.iter_mut().find(|user| user.id == row.id) {
				if let Some(email) = row.email {
					user.emails.push(email);
				}
				continue;
			}
			data.push(tg::User {
				id: row.id,
				emails: row.email.into_iter().collect(),
				namespace: row
					.namespace
					.map(|namespace| namespace.parse())
					.transpose()
					.map_err(|error| tg::error!(!error, "failed to parse the namespace"))?,
				location: None,
			});
		}

		Ok(Some(tg::group::member::list::Output { data }))
	}

	pub(crate) async fn add_group_member(&self, group: &str, user: &str) -> tg::Result<()> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let authentication = &self.context.authentication;
		if authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
		}

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let group = Self::try_get_group_with_transaction(&transaction, group)
			.await?
			.ok_or_else(|| tg::error!("failed to find the group"))?;
		if let Some(Authentication::User(current_user)) = authentication
			&& !Self::user_has_namespace_permission_with_transaction(
				&transaction,
				&current_user.id,
				&group.namespace,
				tg::Permission::Admin,
			)
			.await?
		{
			return Err(tg::error!("unauthorized"));
		}
		let user = Self::try_get_user_with_transaction(&transaction, user)
			.await?
			.ok_or_else(|| tg::error!("failed to find the user"))?;

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				insert into group_members ("group", "user")
				values ({p}1, {p}2)
				on conflict ("group", "user") do nothing;
			"#
		);
		let params = db::params![group.id.to_string(), user.id.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(crate) async fn remove_group_member(
		&self,
		group: &str,
		user: &str,
	) -> tg::Result<Option<()>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let authentication = &self.context.authentication;
		if authentication
			.as_ref()
			.is_none_or(|authentication| authentication.is_runner() || authentication.is_sandbox())
		{
			return Err(tg::error!("unauthorized"));
		}

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(group) = Self::try_get_group_with_transaction(&transaction, group).await? else {
			return Ok(None);
		};
		if let Some(Authentication::User(current_user)) = authentication
			&& !Self::user_has_namespace_permission_with_transaction(
				&transaction,
				&current_user.id,
				&group.namespace,
				tg::Permission::Admin,
			)
			.await?
		{
			return Err(tg::error!("unauthorized"));
		}
		let Some(user) = Self::try_get_user_with_transaction(&transaction, user).await? else {
			return Ok(None);
		};

		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from group_members
				where "group" = {p}1 and "user" = {p}2;
			"#
		);
		let params = db::params![group.id.to_string(), user.id.to_string()];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok((n > 0).then_some(()))
	}

	pub(crate) async fn try_get_group_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		group: &str,
	) -> tg::Result<Option<tg::Group>> {
		if let Ok(id) = group.parse::<tg::group::Id>() {
			Self::try_get_group_by_id_with_transaction(transaction, &id).await
		} else {
			let namespace = group
				.parse::<tg::Namespace>()
				.map_err(|error| tg::error!(!error, "invalid namespace"))?;
			Self::namespace_for_group(&namespace)?;
			Self::try_get_group_by_namespace_with_transaction(transaction, &namespace).await
		}
	}

	pub(crate) async fn try_get_group_by_id_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		id: &tg::group::Id,
	) -> tg::Result<Option<tg::Group>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			namespace: String,
			parent: Option<String>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::group::Id,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select id, namespace, parent
				from groups
				where id = {p}1;
			"
		);
		let row = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		row.map(|row| Self::group_from_row(row.id, &row.namespace, row.parent.as_deref()))
			.transpose()
	}

	pub(crate) async fn try_get_group_by_namespace_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<tg::Group>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			namespace: String,
			parent: Option<String>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::group::Id,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select id, namespace, parent
				from groups
				where namespace = {p}1;
			"
		);
		let row = transaction
			.query_optional_into::<Row>(statement.into(), db::params![namespace.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		row.map(|row| Self::group_from_row(row.id, &row.namespace, row.parent.as_deref()))
			.transpose()
	}

	fn group_from_row(
		id: tg::group::Id,
		namespace: &str,
		parent: Option<&str>,
	) -> tg::Result<tg::Group> {
		Ok(tg::Group {
			id,
			namespace: namespace
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the namespace"))?,
			parent: parent
				.map(str::parse)
				.transpose()
				.map_err(|error| tg::error!(!error, "failed to parse the parent group id"))?,
		})
	}

	pub(crate) fn namespace_for_user(namespace: &tg::Namespace) -> tg::Result<()> {
		if namespace.is_root() || namespace.components().count() != 1 {
			return Err(tg::error!("invalid namespace"));
		}
		Ok(())
	}

	pub(crate) fn namespace_for_group(namespace: &tg::Namespace) -> tg::Result<tg::Namespace> {
		if namespace.is_root() {
			return Err(tg::error!("invalid namespace"));
		}
		Ok(namespace.clone())
	}

	pub(crate) fn parent_namespace(namespace: &tg::Namespace) -> Option<tg::Namespace> {
		let components = namespace
			.components()
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		if components.len() <= 1 {
			return None;
		}
		Some(tg::Namespace::with_components(
			components[..components.len() - 1].iter().cloned(),
		))
	}

	pub(crate) async fn namespace_in_use_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		namespace: &str,
	) -> tg::Result<bool> {
		let p = transaction.p();
		for statement in [
			formatdoc!(
				r"
					select 1
					from users
					where namespace = {p}1;
				"
			),
			formatdoc!(
				r"
					select 1
					from groups
					where namespace = {p}1;
				"
			),
		] {
			if transaction
				.query_optional(statement.into(), db::params![namespace])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				.is_some()
			{
				return Ok(true);
			}
		}
		Ok(false)
	}

	pub(crate) async fn create_group_request(
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
		let output = self
			.create_group(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the group"))?;
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
		Ok(response.body(body).unwrap())
	}

	pub(crate) async fn list_groups_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let output = self
			.list_groups(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the groups"))?;
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
		Ok(response.body(body).unwrap())
	}

	pub(crate) async fn get_or_list_groups_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		if request.uri().query().is_some() {
			self.try_get_group_request(request).await
		} else {
			self.list_groups_request(request).await
		}
	}

	pub(crate) async fn try_get_group_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params::<tg::group::get::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let Some(output) = self
			.try_get_group(&arg.group)
			.await
			.map_err(|error| tg::error!(!error, group = %arg.group, "failed to get the group"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
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
		Ok(response.body(body).unwrap())
	}

	pub(crate) async fn try_delete_group_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params::<tg::group::delete::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let Some(()) = self.try_delete_group(&arg.group).await.map_err(
			|error| tg::error!(!error, group = %arg.group, "failed to delete the group"),
		)?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}
		Ok(http::Response::builder().empty().unwrap().boxed_body())
	}

	pub(crate) async fn list_group_members_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params::<tg::group::member::list::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let Some(output) = self.list_group_members(&arg.group).await.map_err(
			|error| tg::error!(!error, group = %arg.group, "failed to list the group members"),
		)?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
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
		Ok(response.body(body).unwrap())
	}

	pub(crate) async fn add_group_member_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.json::<tg::group::member::add::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.add_group_member(&arg.group, &arg.user)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					group = %arg.group,
					user = %arg.user,
					"failed to add the group member"
				)
			})?;
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}
		Ok(http::Response::builder().empty().unwrap().boxed_body())
	}

	pub(crate) async fn remove_group_member_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params::<tg::group::member::remove::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let Some(()) = self
			.remove_group_member(&arg.group, &arg.user)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					group = %arg.group,
					user = %arg.user,
					"failed to remove the group member"
				)
			})?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}
		Ok(http::Response::builder().empty().unwrap().boxed_body())
	}
}
