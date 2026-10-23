use {crate::Session, tangram_client::prelude::*};

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

#[must_use]
pub fn tokens_valid(tokens: &[tg::authorization::Token], clock: &crate::clock::Clock) -> bool {
	let now = clock.unix_timestamp();
	tokens.is_empty() || tokens.iter().all(|token| token.body.expires_at > now)
}

pub fn remove_expired_tokens(tokens: &mut tg::authorization::Tokens, clock: &crate::clock::Clock) {
	let now = clock.unix_timestamp();
	for token in tokens.remove_local_authorization() {
		if token.body.expires_at > now {
			tokens.insert_local_authorization(token);
		}
	}
}

impl Session {
	pub async fn try_get_cached_remote_response(
		&self,
		remote: &str,
		request: &Request,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<Response>> {
		self.server
			.components
			.remote
			.try_get_cached_remote_response(self, remote, request, ttl)
			.await
	}

	pub async fn put_cached_remote_response(
		&self,
		remote: &str,
		request: &Request,
		response: &Response,
	) -> tg::Result<()> {
		self.server
			.components
			.remote
			.put_cached_remote_response(self, remote, request, response)
			.await
	}

	pub async fn invalidate_remote_cache(&self, remote: &str) {
		self.server
			.components
			.remote
			.invalidate_remote_cache(self, remote)
			.await;
	}
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

#[cfg(test)]
mod tests {
	use tangram_client::prelude::*;

	#[test]
	fn expiration_preserves_other_proofs() {
		let clock = crate::clock::Clock::new(None, None);
		let resource = tg::process::Id::new();
		let token = |expires_at| tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at,
				permissions: vec![tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Node,
				)],
				resource: resource.clone().into(),
			},
			metadata: tg::authorization::Metadata {
				algorithm: tg::authorization::Algorithm::Ed25519,
				key: "default".into(),
			},
			signature: Vec::new(),
		};
		let mut expired = token(i64::MIN);
		expired.body.resource = tg::process::Id::new().into();
		let valid = token(i64::MAX);
		let mut tokens =
			tg::authorization::Tokens::with_authorization([expired.clone(), valid.clone()]);
		let remote = tg::Location::Remote(tg::location::Remote {
			name: "default".into(),
			region: None,
		});
		tokens.insert_authorization(remote.clone(), expired.clone());
		let key =
			tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
				.unwrap();
		let sync = tg::authorization::Token::sign(
			tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Sync(
					tg::authorization::permission::sync::Permission::Read,
				)],
				resource: tg::sync::Id::new().into(),
			},
			&key,
		)
		.unwrap();
		let location = tg::Location::Local(tg::location::Local::default());
		tokens.insert_authorization(location, sync.clone());

		assert!(!super::tokens_valid(tokens.local_authorization(), &clock));
		super::remove_expired_tokens(&mut tokens, &clock);
		assert_eq!(tokens.local_authorization().len(), 2);
		assert!(tokens.local_authorization().contains(&valid));
		assert_eq!(tokens.authorization(&remote), &[expired]);
		assert!(super::tokens_valid(tokens.local_authorization(), &clock));
		assert!(tokens.local_authorization().contains(&sync));
	}
}
