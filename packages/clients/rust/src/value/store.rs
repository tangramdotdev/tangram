use crate::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub location: Option<tg::Location>,
	pub public: bool,
}
