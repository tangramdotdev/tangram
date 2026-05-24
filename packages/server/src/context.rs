use {std::sync::Arc, tangram_client::prelude::*, tangram_futures::task::Stopper};

#[derive(Clone, Debug)]
pub struct Context {
	pub authentication: Option<Authentication>,
	pub id: Option<tg::Id>,
	pub sandbox: bool,
	pub stopper: Option<Stopper>,
}

#[derive(Clone, Debug, derive_more::IsVariant, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Authentication {
	Process(Arc<Process>),
	Root,
	Runner,
	Sandbox(Sandbox),
	User(tg::User),
}

#[derive(Clone, Debug)]
pub struct Process {
	pub created_by: Option<tg::user::Id>,
	pub debug: Option<tg::process::Debug>,
	pub id: tg::process::Id,
	pub location: Option<tg::Location>,
	pub retry: bool,
	pub sandbox: tg::sandbox::Id,
	pub token: String,
}

#[derive(Clone, Debug)]
pub struct Sandbox {
	pub id: tg::sandbox::Id,
	pub location: tg::Location,
}

impl Authentication {
	#[must_use]
	pub fn uses_all_grants(authentication: Option<&Self>) -> bool {
		matches!(
			authentication,
			None | Some(Self::Process(_) | Self::Runner | Self::Sandbox(_))
		)
	}

	#[must_use]
	pub fn user_id(authentication: Option<&Self>) -> Option<&tg::user::Id> {
		let Some(Self::User(user)) = authentication else {
			return None;
		};
		Some(&user.id)
	}
}

impl Context {
	#[must_use]
	pub fn root() -> Self {
		Self {
			authentication: Some(Authentication::Root),
			id: None,
			sandbox: false,
			stopper: None,
		}
	}
}
