use crate::prelude::*;

pub type Selector = tg::Selector<tg::user::Id>;

impl From<tg::user::Id> for Selector {
	fn from(value: tg::user::Id) -> Self {
		Self::Id(value)
	}
}
