use {
	crate::{Session, node::Node, user::parse_selector},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let group = session
						.create_group_with_transaction(transaction, arg)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(tg::group::create::Output {
						group,
					}))
				}
				.boxed()
			})
			.await
	}

	async fn create_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::Group> {
		let node = self
			.create_group_with_ancestors_with_transaction(transaction, &arg.specifier)
			.await?;
		group_from_node(node)
	}

	async fn create_group_with_ancestors_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Node> {
		if Self::try_get_node_by_specifier_with_transaction(transaction, specifier)
			.await?
			.is_some()
		{
			return Err(tg::error!("specifier is already in use"));
		}
		let node = self
			.ensure_group_with_ancestors_with_transaction(transaction, specifier)
			.await?;
		Ok(node)
	}

	pub(crate) async fn ensure_group_with_ancestors_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Node> {
		let components = specifier.components().collect::<Vec<_>>();
		if components.is_empty() {
			return Err(tg::error!("invalid specifier"));
		}
		let mut parent = None;
		let mut node = None;
		for index in 0..components.len() {
			let specifier = tg::Specifier::with_components(
				components[..=index]
					.iter()
					.map(|component| tg::specifier::Component::new((*component).to_owned())),
			);
			if let Some(existing) =
				Self::try_get_node_by_specifier_with_transaction(transaction, &specifier).await?
			{
				if existing.kind != tg::id::Kind::Group {
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
		node.ok_or_else(|| tg::error!("invalid specifier"))
	}

	async fn create_group_node_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		specifier: &tg::Specifier,
		parent: Option<&tg::Id>,
	) -> tg::Result<Node> {
		let id = tg::group::Id::new();
		let node = Self::create_node_with_transaction(
			transaction,
			&id.clone().into(),
			tg::id::Kind::Group,
			specifier,
			parent,
		)
		.await?;
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into groups (id, name, parent)
				values ({p}1, {p}2, {p}3);
			"
		);
		transaction
			.execute(
				statement.into(),
				db::params![
					id.to_string(),
					node.name.clone(),
					node.parent.as_ref().map(ToString::to_string)
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if let Some(principal) = self.write_user_principal() {
			let arg = tg::grant::create::Arg {
				principal,
				permission: tg::grant::Permission::Admin,
				resource: tg::grant::Resource::Id(id.clone().into()),
			};
			self.create_grant_with_transaction(transaction, arg).await?;
		}
		Ok(node)
	}

	pub(crate) async fn list_groups(
		&self,
		arg: tg::group::list::Arg,
	) -> tg::Result<tg::group::list::Output> {
		let _ = arg;
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
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let rows = transaction
			.query_all_into::<Row>(
				"
					select nodes.id, nodes.name, nodes.parent, nodes.specifier
					from nodes
					where kind = 'group'
					order by specifier;
				"
				.into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut data = Vec::new();
		for row in rows {
			if self
				.node_is_visible_with_transaction(&transaction, &row.id)
				.await?
			{
				data.push(tg::Group {
					id: row.id.try_into()?,
					name: row.name,
					parent: row.parent,
					specifier: row.specifier,
				});
			}
		}
		Ok(tg::group::list::Output { data })
	}

	pub(crate) async fn try_get_group(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<Option<tg::Group>> {
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
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(&transaction, group).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Group
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		group_from_node(node).map(Some)
	}

	pub(crate) async fn try_delete_group(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<Option<()>> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let group = group.clone();
				let session = session.clone();
				async move {
					let output = session
						.delete_group_with_transaction(transaction, &group)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await
	}

	async fn delete_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
	) -> tg::Result<Option<()>> {
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(transaction, group).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Group {
			return Ok(None);
		}
		let p = transaction.p();
		for statement in [
			format!("delete from group_members where \"group\" = {p}1 or member = {p}1;"),
			format!("delete from organization_members where member = {p}1;"),
			format!("delete from grants where resource = {p}1 or principal = {p}1;"),
			format!("delete from visibility where resource = {p}1 or principal = {p}1;"),
			format!("delete from groups where id = {p}1;"),
			format!("delete from nodes where id = {p}1;"),
		] {
			transaction
				.execute(statement.into(), db::params![node.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(Some(()))
	}

	pub(crate) async fn list_group_members(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<tg::group::members::list::Output> {
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
		let group = Self::try_get_node_by_selector_with_transaction(&transaction, group)
			.await?
			.ok_or_else(|| tg::error!("failed to find the group"))?;
		if group.kind != tg::id::Kind::Group
			|| !self
				.node_is_visible_with_transaction(&transaction, &group.id)
				.await?
		{
			return Err(tg::error!("failed to find the group"));
		}
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select member
				from group_members
				where "group" = {p}1
				order by member;
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![group.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| row.member.try_into())
			.collect::<tg::Result<_>>()?;
		Ok(tg::group::members::list::Output { data })
	}

	pub(crate) async fn add_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<()> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let group = group.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					session
						.add_group_member_with_transaction(transaction, &group, &member)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await
	}

	pub(crate) async fn add_group_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<()> {
		let group = Self::try_get_node_by_selector_with_transaction(transaction, group)
			.await?
			.ok_or_else(|| tg::error!("failed to find the group"))?;
		if group.kind != tg::id::Kind::Group {
			return Err(tg::error!("failed to find the group"));
		}
		let member_id: tg::Id = member.clone().into();
		if Self::try_get_node_by_id_with_transaction(transaction, &member_id)
			.await?
			.is_none()
		{
			return Err(tg::error!("failed to find the member"));
		}
		if matches!(member, tg::group::Member::Group(_))
			&& group_contains_group_with_transaction(transaction, &member_id, &group.id).await?
		{
			return Err(tg::error!("membership cycle"));
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				insert into group_members ("group", member)
				values ({p}1, {p}2)
				on conflict ("group", member) do nothing;
			"#
		);
		transaction
			.execute(
				statement.into(),
				db::params![group.id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let arg = tg::grant::create::Arg {
			principal: member_to_principal(member),
			permission: tg::grant::Permission::Read,
			resource: tg::grant::Resource::Id(group.id),
		};
		self.create_grant_with_transaction(transaction, arg).await?;
		Ok(())
	}

	pub(crate) async fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<Option<()>> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let group = group.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					let output = session
						.remove_group_member_with_transaction(transaction, &group, &member)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await
	}

	async fn remove_group_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<Option<()>> {
		let Some(group) =
			Self::try_get_node_by_selector_with_transaction(transaction, group).await?
		else {
			return Ok(None);
		};
		let member_id: tg::Id = member.clone().into();
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from group_members
				where "group" = {p}1 and member = {p}2;
			"#
		);
		let deleted = transaction
			.execute(
				statement.into(),
				db::params![group.id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if deleted == 0 {
			return Ok(None);
		}
		let arg = tg::grant::delete::Arg {
			principal: member_to_principal(member),
			permission: tg::grant::Permission::Read,
			resource: tg::grant::Resource::Id(group.id),
		};
		self.delete_grant_with_transaction(transaction, arg).await?;
		Ok(Some(()))
	}

	pub(crate) async fn try_get_group_grants(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::grants::Arg,
	) -> tg::Result<Option<tg::group::grants::Output>> {
		let _ = arg;
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
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(&transaction, group).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Group
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		let data = Self::list_direct_grants_with_transaction(&transaction, &node.id).await?;
		Ok(Some(tg::group::grants::Output { data }))
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
		let output = self.create_group(arg).await?;
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
		let output = self.list_groups(arg).await?;
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

	pub(crate) async fn try_get_group_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let group = parse_selector::<tg::group::Id>(group)?;
		let Some(output) = self.try_get_group(&group).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
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
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn try_delete_group_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		drop(request);
		let group = parse_selector::<tg::group::Id>(group)?;
		let Some(()) = self.try_delete_group(&group).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn list_group_members_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let group = parse_selector::<tg::group::Id>(group)?;
		let output = self.list_group_members(&group).await?;
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

	pub(crate) async fn add_group_member_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg: tg::group::members::add::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let group = parse_selector::<tg::group::Id>(group)?;
		self.add_group_member(&group, &arg.member).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn remove_group_member_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
		member: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let _ = request;
		let group = parse_selector::<tg::group::Id>(group)?;
		let member = member.replace(':', "/").parse()?;
		let Some(()) = self.remove_group_member(&group, &member).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn try_get_group_grants_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or(tg::group::grants::Arg { location: None });
		let group = parse_selector::<tg::group::Id>(group)?;
		let Some(output) = self.try_get_group_grants(&group, arg).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
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
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}

	fn write_user_principal(&self) -> Option<tg::grant::Principal> {
		match self.context.authentication.as_ref() {
			Some(crate::context::Authentication::User(user)) => {
				Some(tg::grant::Principal::User(user.id.clone()))
			},
			_ => None,
		}
	}
}

fn group_from_node(node: Node) -> tg::Result<tg::Group> {
	Ok(tg::Group {
		id: node.id.try_into()?,
		name: node.name,
		parent: node.parent,
		specifier: node.specifier,
	})
}

fn member_to_principal(member: &tg::group::Member) -> tg::grant::Principal {
	match member {
		tg::group::Member::Group(id) => tg::grant::Principal::Group(id.clone()),
		tg::group::Member::User(id) => tg::grant::Principal::User(id.clone()),
	}
}

pub(crate) fn organization_member_to_principal(
	member: &tg::organization::Member,
) -> tg::grant::Principal {
	match member {
		tg::organization::Member::Group(id) => tg::grant::Principal::Group(id.clone()),
		tg::organization::Member::User(id) => tg::grant::Principal::User(id.clone()),
	}
}

async fn group_contains_group_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	group: &tg::Id,
	member: &tg::Id,
) -> tg::Result<bool> {
	#[derive(db::row::Deserialize)]
	struct Row {
		#[tangram_database(as = "db::value::FromStr")]
		member: tg::Id,
	}
	let mut stack = vec![group.clone()];
	let mut visited = std::collections::BTreeSet::new();
	while let Some(group) = stack.pop() {
		if !visited.insert(group.clone()) {
			continue;
		}
		if &group == member {
			return Ok(true);
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select member
				from group_members
				where "group" = {p}1;
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![group.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		stack.extend(rows.into_iter().map(|row| row.member));
	}
	Ok(false)
}
