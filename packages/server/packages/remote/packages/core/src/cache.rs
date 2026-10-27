use tangram_client::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Request {
	Get(GetRequest),
	GroupGet(GroupGetRequest),
	List(ListRequest),
	Match(MatchRequest),
	OrganizationGet(OrganizationGetRequest),
	SandboxGet(SandboxGetRequest),
	TagGet(TagGetRequest),
	UserGet(UserGetRequest),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Response {
	Get(GetResponse),
	GroupGet(GroupGetResponse),
	List(ListResponse),
	Match(MatchResponse),
	OrganizationGet(OrganizationGetResponse),
	SandboxGet(SandboxGetResponse),
	TagGet(TagGetResponse),
	UserGet(UserGetResponse),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetRequest {
	pub arg: tg::get::Arg,
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetResponse {
	pub output: Option<tg::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GroupGetRequest {
	pub arg: tg::group::get::Arg,
	pub id: tg::group::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GroupGetResponse {
	pub output: Option<tg::group::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ListRequest {
	pub arg: tg::list::Arg,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ListResponse {
	pub output: tg::list::Output,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct MatchRequest {
	pub arg: tg::match_::Arg,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct MatchResponse {
	pub output: tg::match_::Output,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct OrganizationGetRequest {
	pub arg: tg::organization::get::Arg,
	pub id: tg::organization::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct OrganizationGetResponse {
	pub output: Option<tg::organization::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SandboxGetRequest {
	pub arg: tg::sandbox::get::Arg,
	pub id: tg::sandbox::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SandboxGetResponse {
	pub output: Option<tg::sandbox::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TagGetRequest {
	pub arg: tg::tag::get::Arg,
	pub id: tg::tag::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TagGetResponse {
	pub output: Option<tg::tag::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct UserGetRequest {
	pub arg: tg::user::get::Arg,
	pub id: tg::user::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct UserGetResponse {
	pub output: Option<tg::user::get::Output>,
}

impl Request {
	#[must_use]
	pub fn matches_response(&self, response: &Response) -> bool {
		matches!(
			(self, response),
			(Self::Get(_), Response::Get(_))
				| (Self::GroupGet(_), Response::GroupGet(_))
				| (Self::List(_), Response::List(_))
				| (Self::Match(_), Response::Match(_))
				| (Self::OrganizationGet(_), Response::OrganizationGet(_))
				| (Self::SandboxGet(_), Response::SandboxGet(_))
				| (Self::TagGet(_), Response::TagGet(_))
				| (Self::UserGet(_), Response::UserGet(_))
		)
	}
}
