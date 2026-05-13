use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_get_user_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		user: &str,
	) -> tg::Result<Option<tg::User>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			email: Option<String>,
			handle: Option<String>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
		}

		let p = transaction.p();
		let (where_, param) = if let Ok(id) = user.parse::<tg::user::Id>() {
			("users.id", id.to_string())
		} else {
			("users.handle", user.to_owned())
		};
		let statement = formatdoc!(
			"
				select users.id, users.handle, user_emails.email
				from users
				left join user_emails on user_emails.\"user\" = users.id
				where {where_} = {p}1
				order by user_emails.email;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![param])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(first) = rows.first() else {
			return Ok(None);
		};
		let id = first.id.clone();
		let handle = first.handle.clone();
		let user = tg::User {
			id,
			emails: rows.into_iter().filter_map(|row| row.email).collect(),
			handle,
			location: None,
		};
		Ok(Some(user))
	}
}
