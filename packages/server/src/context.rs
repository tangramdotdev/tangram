use {crate::authentication::Authentication, tangram_futures::task::Stopper};

#[derive(Clone, Debug)]
pub struct Context {
	pub authentication: Option<Authentication>,
	pub id: Option<String>,
	pub sandbox: bool,
	pub stopper: Option<Stopper>,
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
