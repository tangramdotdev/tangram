use crate::Id;
use url::Url;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Login {
	pub id: Id,
	pub url: Url,
	pub token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct User {
	pub id: Id,
	pub email: String,
	pub token: Option<Id>,
}
